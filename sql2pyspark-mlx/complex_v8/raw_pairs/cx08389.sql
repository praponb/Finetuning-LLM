WITH valid_adjusters AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        adjuster_name,
        team,
        level,
        CONCAT_WS(' / ', team, adjuster_name) AS team_adjuster_name_label
    FROM adjusters
    WHERE level >= 3
      AND team = 'Property'
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.reported_at) FROM claims c WHERE c.adjuster_id = s.id) AS last_claim_reported_at
    FROM valid_adjusters s
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM summarized_rows s
    WHERE s.id IN (
            SELECT c.adjuster_id
            FROM claims c
            WHERE c.claim_type = 'fire'
              AND c.approved_amount <= 14993.0
        )
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(level - 3) AS level_distance,
        CONCAT_WS('-', adjuster_name, team) AS adjuster_name_team_label,
        LENGTH(TRIM(adjuster_name)) AS adjuster_name_len
    FROM screened
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.id) AS claim_count
    FROM labeled s
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        adjuster_name,
        team,
        level,
        team_adjuster_name_label,
        last_claim_reported_at,
        level_distance,
        adjuster_name_len,
        claim_count,
        ROUND(adjuster_name_len / NULLIF(SUM(adjuster_name_len) OVER (PARTITION BY level, claim_count), 0), 4) AS adjuster_name_len_share_of_level_claim_count,
        LEAD(level, 3) OVER (ORDER BY adjuster_name_len, id) AS next3_level,
        ROUND(PERCENT_RANK() OVER (ORDER BY adjuster_name_len), 3) AS adjuster_name_len_percent_rank,
        SUM(level_distance) OVER w_level_claim_count AS level_claim_count_sum_level_distance,
        ROUND(claim_count / NULLIF(SUM(claim_count) OVER (PARTITION BY level, claim_count), 0), 4) AS claim_count_share_of_level_claim_count
    FROM with_stats
    WINDOW w_level_claim_count AS (PARTITION BY level, claim_count)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, reading
FROM (
    SELECT id, adjuster_name_len_share_of_level_claim_count, adjuster_name_len_percent_rank
    FROM with_trends
) src
UNPIVOT (
    reading FOR metric_name IN (adjuster_name_len_share_of_level_claim_count, adjuster_name_len_percent_rank)
) unp
ORDER BY id, metric_name
