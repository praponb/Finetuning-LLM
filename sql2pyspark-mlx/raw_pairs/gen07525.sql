WITH kept_keys AS (
    -- Set difference on keys
    SELECT plan
    FROM subscriptions
    EXCEPT DISTINCT
    SELECT plan
    FROM subscriptions
    WHERE plan != 'standard'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM subscriptions s
    JOIN kept_keys k ON s.plan = k.plan
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        start_date,
        end_date,
        monthly_fee,
        CUME_DIST() OVER (PARTITION BY customer_id ORDER BY monthly_fee) AS monthly_fee_cume_dist
    FROM reconciled
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY monthly_fee DESC NULLS LAST) AS monthly_fee_percent_rank,
        PERCENT_RANK() OVER (ORDER BY monthly_fee_cume_dist NULLS LAST) AS monthly_fee_cume_dist_percent_rank,
        DENSE_RANK() OVER (PARTITION BY YEAR(end_date) ORDER BY monthly_fee DESC NULLS LAST) AS monthly_fee_dense_rank,
        PERCENT_RANK() OVER (ORDER BY monthly_fee_cume_dist DESC) AS monthly_fee_cume_dist_percent_rank_2,
        DENSE_RANK() OVER (ORDER BY monthly_fee_cume_dist DESC NULLS FIRST) AS monthly_fee_cume_dist_dense_rank
    FROM with_windows
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        start_date,
        end_date,
        monthly_fee,
        monthly_fee_cume_dist,
        monthly_fee_percent_rank,
        monthly_fee_cume_dist_percent_rank,
        monthly_fee_dense_rank,
        monthly_fee_cume_dist_percent_rank_2,
        LEAST(monthly_fee_cume_dist, monthly_fee_dense_rank) AS least_monthly_fee_cume_dist_monthly_fee_dense_rank,
        COALESCE(monthly_fee_cume_dist_percent_rank, 0) AS monthly_fee_cume_dist_percent_rank_filled
    FROM metrics
    WHERE monthly_fee_percent_rank > 0.59
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        start_date,
        end_date,
        monthly_fee,
        monthly_fee_cume_dist,
        monthly_fee_percent_rank,
        monthly_fee_dense_rank,
        least_monthly_fee_cume_dist_monthly_fee_dense_rank,
        DENSE_RANK() OVER (ORDER BY monthly_fee_cume_dist_percent_rank_2 DESC NULLS FIRST) AS monthly_fee_cume_dist_percent_rank_2_dense_rank
    FROM base
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN least_monthly_fee_cume_dist_monthly_fee_dense_rank >= 77 THEN 'PREMIUM' ELSE 'STANDARD' END AS least_monthly_fee_cume_dist_monthly_fee_dense_rank_tier,
    SUM(least_monthly_fee_cume_dist_monthly_fee_dense_rank) AS total_least_monthly_fee_cume_dist_monthly_fee_dense_rank,
    ROUND(AVG(monthly_fee_cume_dist), 2) AS avg_monthly_fee_cume_dist,
    AVG(monthly_fee_dense_rank) AS avg_monthly_fee_dense_rank
FROM scored
GROUP BY least_monthly_fee_cume_dist_monthly_fee_dense_rank_tier
ORDER BY avg_monthly_fee_dense_rank DESC NULLS FIRST
