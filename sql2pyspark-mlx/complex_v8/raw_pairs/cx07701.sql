WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        status,
        DAYOFWEEK(claim_date) AS claim_date_dayofweek,
        SUBSTRING(TRIM(claim_type), 1, 2) AS claim_type_prefix
    FROM claims
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        adjuster_id,
        claim_type,
        claimed_amount,
        status,
        claim_type_prefix,
        COUNT(*) OVER (PARTITION BY status) AS rows_in_status,
        ROUND(CUME_DIST() OVER (PARTITION BY adjuster_id, status ORDER BY claimed_amount DESC, id), 3) AS claimed_amount_cume_dist,
        SUM(claim_date_dayofweek) OVER w_status AS status_sum_claim_date_dayofweek,
        MIN(claimed_amount) OVER w_status AS status_min_claimed_amount,
        FIRST_VALUE(claimed_amount) OVER (ORDER BY claim_date_dayofweek ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claimed_amount,
        LAG(claim_date_dayofweek) OVER (PARTITION BY adjuster_id, status ORDER BY claimed_amount DESC, id) AS prev_claim_date_dayofweek,
        ROUND(claim_date_dayofweek / NULLIF(SUM(claim_date_dayofweek) OVER (PARTITION BY status), 0), 4) AS claim_date_dayofweek_share_of_status,
        NTH_VALUE(claim_date_dayofweek, 2) OVER (PARTITION BY adjuster_id, status ORDER BY claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date_dayofweek,
        FIRST_VALUE(claim_date_dayofweek) OVER (ORDER BY claim_date_dayofweek ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claim_date_dayofweek,
        DENSE_RANK() OVER (ORDER BY claim_date_dayofweek ASC) AS claim_date_dayofweek_dense_rank
    FROM valid
    WINDOW w_status AS (PARTITION BY status)
),
leaders AS (
    -- First row per group
    SELECT *
    FROM metrics
    WHERE claim_date_dayofweek_dense_rank = 1
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(status) OVER w_claim_type) AS distinct_status_in_claim_type,
        LEAD(claimed_amount, 1, 0) OVER (PARTITION BY adjuster_id, prev_claim_date_dayofweek ORDER BY rows_in_status DESC, id) AS next_claimed_amount,
        SUM(claimed_amount) OVER (PARTITION BY claim_type) AS claim_type_sum_claimed_amount
    FROM leaders
    WINDOW w_claim_type AS (PARTITION BY claim_type)
)
-- Final projection
SELECT
    id,
    adjuster_id,
    claim_type,
    claimed_amount,
    claim_type_prefix,
    claimed_amount_cume_dist,
    status_sum_claim_date_dayofweek,
    status_min_claimed_amount,
    first_claimed_amount,
    prev_claim_date_dayofweek,
    second_claim_date_dayofweek,
    first_claim_date_dayofweek,
    claim_date_dayofweek_dense_rank,
    distinct_status_in_claim_type,
    next_claimed_amount,
    claim_type_sum_claimed_amount
FROM windowed
ORDER BY distinct_status_in_claim_type NULLS LAST, status_min_claimed_amount NULLS LAST
