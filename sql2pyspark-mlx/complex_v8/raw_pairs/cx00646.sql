WITH filtered_policys AS (
    -- Filter policies
    SELECT *
    FROM policies
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_id ORDER BY premium), 3) AS premium_percent_rank,
        NTH_VALUE(start_date, 2) OVER (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date
    FROM filtered_policys
),
holder_id_stats AS (
    -- Max premium per holder_id
    SELECT holder_id, MAX(premium) AS max_premium
    FROM metrics
    GROUP BY holder_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_premium,
        ROUND(s.premium - g.max_premium, 2) AS premium_minus_max
    FROM metrics s
    JOIN holder_id_stats g ON g.holder_id = s.holder_id
    WHERE s.premium = g.max_premium
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.status,
        s.premium_percent_rank,
        s.max_premium,
        s.premium_minus_max,
        c.id AS claim_id,
        c.adjuster_id AS claim_adjuster_id,
        c.claimed_amount AS claim_claimed_amount
    FROM above_average s
    JOIN claims c ON c.policy_id = s.id
)
SELECT
    holder_id,
    claim_adjuster_id,
    MAX(premium_minus_max) AS max_premium_minus_max,
    MIN(start_date) AS first_start_date
FROM detail_rows
GROUP BY ROLLUP(holder_id, claim_adjuster_id)
