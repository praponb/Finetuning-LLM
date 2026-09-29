WITH base_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        CASE WHEN premium >= 3465.4 THEN 'A' WHEN premium >= 2096.6 THEN 'B' ELSE 'C' END AS premium_tier
    FROM policies
    WHERE status IN ('ACTIVE', 'EXPIRED')
      AND start_date IS NOT NULL
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(premium) OVER (PARTITION BY status, DATE_TRUNC('MONTH', end_date) ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_premium_so_far,
        ROUND(CUME_DIST() OVER (ORDER BY end_date ASC NULLS LAST, id), 3) AS end_date_cume_dist,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY product), 0), 4) AS premium_share_of_product,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY product)) AS distinct_status_in_product,
        ROUND(CUME_DIST() OVER w_status_end_date_month_by_end_2, 3) AS end_date_cume_dist_2,
        ROW_NUMBER() OVER (PARTITION BY MONTH(end_date) ORDER BY premium DESC, id) AS rn,
        NTH_VALUE(end_date, 2) OVER (ORDER BY end_date ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date,
        ROW_NUMBER() OVER (ORDER BY end_date ASC NULLS LAST, id) AS end_date_seq,
        LEAD(end_date, 2) OVER (ORDER BY end_date ASC NULLS LAST, id) AS next2_end_date
    FROM base_policys
    WINDOW w_status_end_date_month_by_end_2 AS (PARTITION BY status, DATE_TRUNC('MONTH', end_date) ORDER BY end_date DESC)
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        premium_tier,
        max_premium_so_far,
        end_date_cume_dist,
        premium_share_of_product,
        distinct_status_in_product,
        end_date_cume_dist_2,
        second_end_date,
        end_date_seq,
        next2_end_date
    FROM windowed
    WHERE rn = 1
),
windowed_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(distinct_status_in_product) OVER (PARTITION BY holder_id, YEAR(second_end_date) ORDER BY next2_end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_distinct_status_in_product,
        LAG(end_date_seq, 2) OVER (PARTITION BY holder_id, YEAR(second_end_date) ORDER BY next2_end_date, id) AS prev2_end_date_seq,
        DENSE_RANK() OVER (PARTITION BY holder_id, YEAR(second_end_date) ORDER BY next2_end_date, id) AS next2_end_date_dense_rank,
        DENSE_RANK() OVER (PARTITION BY holder_id, YEAR(second_end_date) ORDER BY next2_end_date) AS next2_end_date_dense_rank_3
    FROM top_per_group
)
SELECT
    id,
    holder_id,
    premium,
    end_date,
    status,
    premium_tier,
    max_premium_so_far,
    end_date_cume_dist,
    premium_share_of_product,
    distinct_status_in_product,
    end_date_cume_dist_2,
    end_date_seq,
    next2_end_date,
    next2_end_date_dense_rank_3,
    ABS(prev2_end_date_seq - 7) AS prev2_end_date_seq_distance
FROM windowed_2
ORDER BY max_premium_so_far DESC, premium_share_of_product
