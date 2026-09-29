WITH base_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        SUBSTRING(TRIM(status), 1, 2) AS status_prefix
    FROM policies
    WHERE product IN ('LIFE', 'AUTO', 'HEALTH')
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        premium,
        status_or_unknown,
        COUNT(*) OVER w_status_or_unknown_holder_id AS rows_in_status_or_unknown_holder_id,
        ROW_NUMBER() OVER (PARTITION BY holder_id, product ORDER BY premium DESC, id) AS premium_seq,
        FIRST_VALUE(premium) IGNORE NULLS OVER (ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium,
        RANK() OVER (PARTITION BY product ORDER BY premium, id) AS premium_rank,
        MIN(premium) OVER w_status_or_unknown_holder_id AS status_or_unknown_holder_id_min_premium,
        MAX(premium) OVER (PARTITION BY status_or_unknown, holder_id) AS status_or_unknown_holder_id_max_premium,
        ROUND(AVG(premium) OVER (PARTITION BY holder_id, product ORDER BY premium DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_6_prior,
        SUM(premium) OVER (PARTITION BY product ORDER BY premium, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS premium_sum_3,
        NTH_VALUE(start_date, 2) OVER (ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        ROUND(premium / NULLIF(SUM(premium) OVER w_status_or_unknown_holder_id, 0), 4) AS premium_share_of_status_or_unknown_holder_id,
        ROUND(AVG(premium) OVER (ORDER BY start_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS premium_avg_3,
        LAST_VALUE(premium) IGNORE NULLS OVER (PARTITION BY product ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_premium,
        RANK() OVER (PARTITION BY product ORDER BY premium) AS premium_rank_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_id, product ORDER BY premium DESC, id), 3) AS premium_percent_rank
    FROM base_policys
    WINDOW w_status_or_unknown_holder_id AS (PARTITION BY status_or_unknown, holder_id), w_product_by_premium AS (PARTITION BY product ORDER BY premium, id)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        premium,
        rows_in_status_or_unknown_holder_id,
        premium_seq,
        first_known_premium,
        premium_rank,
        premium_avg_6_prior,
        premium_sum_3,
        second_start_date,
        last_known_premium,
        premium_rank_2,
        premium_percent_rank,
        NTILE(10) OVER (PARTITION BY status_or_unknown, premium_seq ORDER BY premium_sum_3, id) AS premium_sum_3_decile,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY status_or_unknown, premium_rank)) AS distinct_holder_id_in_status_or_unknown_premium_,
        NTILE(3) OVER (PARTITION BY premium_rank_2 ORDER BY premium_avg_3 DESC, id) AS premium_avg_3_tercile,
        ROUND(AVG(premium_avg_3) OVER (ORDER BY premium_avg_3 ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS premium_avg_3_avg_2,
        LAST_VALUE(premium) IGNORE NULLS OVER (PARTITION BY premium_seq, status_or_unknown ORDER BY premium_sum_3, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_premium_2,
        MAX(premium_avg_6_prior) OVER (PARTITION BY status_or_unknown, premium_rank) AS status_or_unknown_premium_rank_max_premium_avg_6
    FROM windowed
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        premium_seq,
        premium_avg_3_tercile,
        holder_id,
        PERCENTILE_APPROX(distinct_holder_id_in_status_or_unknown_premium_, 0.5) AS median_distinct_holder_id_in_status_or_unknown_p,
        MIN(second_start_date) AS first_second_start_date
    FROM with_trends
    GROUP BY 1, 2, 3
)
SELECT
    premium_seq,
    premium_avg_3_tercile,
    holder_id,
    first_second_start_date,
    ROUND(premium_avg_3_tercile / NULLIF(premium_seq, 0), 3) AS premium_avg_3_tercile_per_premium_seq
FROM summary
ORDER BY premium_avg_3_tercile_per_premium_seq, holder_id
