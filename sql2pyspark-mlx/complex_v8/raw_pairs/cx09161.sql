WITH filtered AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        LENGTH(TRIM(status)) AS status_len
    FROM claims
    WHERE MONTH(reported_at) IN (1, 2)
      AND claimed_amount BETWEEN 11235.6 AND 45826.9
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        FLOOR(status_len / 2) * 2 AS status_len_bucket,
        INITCAP(TRIM(status)) AS status_clean,
        COALESCE(status_len, 0) AS status_len_filled
    FROM filtered
),
without_matches AS (
    -- Keep rows whose adjuster matches
    SELECT s.*
    FROM tagged s
    WHERE EXISTS (
            SELECT 1
            FROM adjusters a
            WHERE a.id = s.adjuster_id
              AND a.level <= 3
        )
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER w_all_by_status_len_bucket_tie AS status_len_bucket_dense_rank,
        LAG(status_len, 1, 0) OVER (ORDER BY status_len_bucket DESC, id) AS prev_status_len,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY status_len, TO_DATE(reported_at)), 2) AS claimed_amount_vs_status_len_reported_at_to_date,
        DENSE_RANK() OVER w_all_by_status_len_bucket AS status_len_bucket_dense_rank_2
    FROM without_matches
    WINDOW w_all_by_status_len_bucket_tie AS (ORDER BY status_len_bucket DESC), w_all_by_status_len_bucket AS (ORDER BY status_len_bucket DESC, id)
),
exploded_detail AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.status,
        s.status_len_filled,
        s.status_len_bucket_dense_rank,
        s.prev_status_len,
        s.status_len_bucket_dense_rank_2,
        c.id AS claim_id,
        c.claimed_amount AS claim_claimed_amount,
        c.approved_amount AS claim_approved_amount
    FROM sequenced s
    JOIN claims c ON c.adjuster_id = s.adjuster_id AND c.claimed_amount > 18364.1
),
joined_adjuster AS (
    -- Enrich with dimension data
    SELECT s.*, a.level AS adjuster_level
    FROM exploded_detail s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id AND a.team IN ('Travel', 'Motor', 'Property')
)
SELECT *
FROM (
    SELECT status_len_filled, status_len_bucket_dense_rank_2, status, adjuster_level
    FROM joined_adjuster
) src
PIVOT (
    MIN(adjuster_level)
    FOR status IN ('IN_REVIEW', 'REJECTED', 'OPEN', 'APPROVED')
)
