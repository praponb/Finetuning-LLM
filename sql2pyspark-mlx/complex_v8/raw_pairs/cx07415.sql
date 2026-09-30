WITH recent_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        CASE WHEN status IN ('CANCELLED', 'LAPSED') THEN 'group_a' WHEN status = 'EXPIRED' THEN 'group_b' ELSE 'other' END AS status_group,
        ABS(premium - 2824.5) AS premium_distance,
        LENGTH(TRIM(status)) AS status_len
    FROM policies
    WHERE holder_id IS NOT NULL
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        premium,
        start_date,
        status,
        premium_distance,
        status_len,
        ROUND(status_len - AVG(status_len) OVER w_holder_id, 2) AS status_len_vs_holder_id_avg,
        RANK() OVER w_status_len_by_status_len_tie AS status_len_rank,
        NTH_VALUE(status_len, 2) OVER (PARTITION BY status_len ORDER BY status_len ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_status_len,
        ROUND(CUME_DIST() OVER w_status_len_by_status_len_tie, 3) AS status_len_cume_dist
    FROM recent_policys
    WINDOW w_holder_id AS (PARTITION BY holder_id), w_status_len_by_status_len_tie AS (PARTITION BY status_len ORDER BY status_len ASC)
),
enriched AS (
    -- Derive helper columns
    SELECT *, ROUND(status_len_cume_dist * 1.1, 2) AS status_len_cume_dist_adj
    FROM with_trends
    WHERE start_date >= DATE '2025-01-05'
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.premium,
        s.start_date,
        s.status,
        s.premium_distance,
        s.status_len_rank,
        s.second_status_len,
        s.status_len_cume_dist,
        s.status_len_cume_dist_adj,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.claimed_amount AS claim_claimed_amount
    FROM enriched s
    JOIN claims c ON c.policy_id = s.id
)
-- Final projection
SELECT
    id,
    premium,
    start_date,
    premium_distance,
    status_len_rank,
    second_status_len,
    status_len_cume_dist,
    status_len_cume_dist_adj,
    claim_approved_amount,
    claim_claimed_amount,
    DATEDIFF(DATE '2025-06-30', start_date) AS days_since_start_date
FROM detail_rows
ORDER BY claim_approved_amount, start_date DESC NULLS FIRST
