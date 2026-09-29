WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        DATE_TRUNC('WEEK', claim_date) AS claim_date_week,
        reported_at + INTERVAL 2 HOURS AS reported_at_plus_2h
    FROM claims
    WHERE claim_type = 'medical'
),
with_activity AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
        )
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(approved_amount) OVER w_policy_id_status, 2) AS policy_id_status_avg_approved_amount,
        DENSE_RANK() OVER (PARTITION BY policy_id ORDER BY reported_at_plus_2h) AS reported_at_plus_2h_dense_rank,
        MIN(claimed_amount) OVER (PARTITION BY policy_id, DATE_TRUNC('MONTH', claim_date_week) ORDER BY approved_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_claimed_amount_so_far
    FROM with_activity
    WINDOW w_policy_id_status AS (PARTITION BY policy_id, status)
)
-- Subtotals with GROUPING SETS
SELECT
    claim_type,
    reported_at_plus_2h_dense_rank,
    policy_id,
    GROUPING(claim_type) AS is_total_claim_type,
    ARRAY_SORT(COLLECT_SET(status)) AS status_list,
    ROUND(SUM(claimed_amount), 2) AS total_claimed_amount
FROM sequenced
GROUP BY GROUPING SETS ((claim_type, reported_at_plus_2h_dense_rank, policy_id), (claim_type), (reported_at_plus_2h_dense_rank), ())
ORDER BY claim_type NULLS LAST, reported_at_plus_2h_dense_rank NULLS LAST, policy_id NULLS LAST
