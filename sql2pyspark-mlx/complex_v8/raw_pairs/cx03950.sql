WITH filtered AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level,
        SUBSTRING(TRIM(adjuster_name), 1, 4) AS adjuster_name_prefix,
        CONCAT(adjuster_name, '#', CAST(id AS STRING)) AS adjuster_name_ref
    FROM adjusters
    WHERE level < 3
      AND UPPER(TRIM(adjuster_name)) LIKE 'H%'
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        adjuster_name,
        level,
        adjuster_name_ref,
        CONCAT_WS('|', team, adjuster_name_prefix) AS team_adjuster_name_prefix_label,
        SUBSTRING(TRIM(adjuster_name_ref), 1, 2) AS adjuster_name_ref_prefix
    FROM filtered
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        MAX(reported_at) AS last_claim_reported_at,
        COUNT(*) AS claim_count
    FROM claims
    GROUP BY adjuster_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.last_claim_reported_at,
        COALESCE(c.claim_count, 0) AS claim_count
    FROM labeled s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER w_all_by_claim_count AS claim_count_dense_rank,
        ROUND(level / NULLIF(SUM(level) OVER (PARTITION BY TO_DATE(last_claim_reported_at), level), 0), 4) AS level_share_of_last_claim_reported_at_to_date_le
    FROM summarized_rows
    WINDOW w_all_by_claim_count AS (ORDER BY claim_count ASC NULLS LAST, id)
)
-- Subtotals with GROUPING SETS
SELECT
    claim_count,
    claim_count_dense_rank,
    COUNT(*) AS cnt,
    ARRAY_SORT(COLLECT_SET(adjuster_name)) AS adjuster_name_list
FROM windowed
GROUP BY GROUPING SETS ((claim_count, claim_count_dense_rank), (claim_count))
