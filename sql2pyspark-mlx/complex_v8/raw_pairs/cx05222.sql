WITH valid_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        CONCAT_WS('-', product, status) AS product_status_label,
        DATEDIFF(DATE '2026-01-01', end_date) AS days_since_end_date
    FROM policies
    WHERE holder_id >= 17
      AND status IN ('ACTIVE', 'EXPIRED')
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        product_status_label,
        days_since_end_date,
        LAG(days_since_end_date, 2) OVER (PARTITION BY product, MONTH(end_date) ORDER BY end_date, id) AS prev2_days_since_end_date,
        MAX(premium) OVER (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_premium_so_far,
        MIN(days_since_end_date) OVER w_product AS product_min_days_since_end_date
    FROM valid_policys
    WINDOW w_product AS (PARTITION BY product)
),
per_group AS (
    -- Group-level totals
    SELECT
        holder_id,
        product,
        COUNT(*) AS row_count,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(CASE WHEN product IN ('HEALTH', 'LIFE') THEN premium ELSE 0 END) AS premium_product_in_health_life,
        SUM(premium) AS total_premium,
        COUNT(DISTINCT holder_id) AS distinct_holder_ids
    FROM sequenced
    GROUP BY 1, 2
    HAVING COUNT(*) >= 2
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        holder_id,
        product,
        row_count,
        premium_product_in_health_life,
        total_premium,
        LAG(distinct_ids, 1, 0) OVER (PARTITION BY product ORDER BY row_count DESC, holder_id, product) AS prev_distinct_ids,
        ROUND(AVG(row_count) OVER (ORDER BY total_premium ASC, holder_id, product ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS row_count_avg_7
    FROM per_group
)
-- Native SQL UNPIVOT (wide to long)
SELECT holder_id, product, measure, metric_value
FROM (
    SELECT holder_id, product, row_count_avg_7, premium_product_in_health_life, total_premium
    FROM ranked
) src
UNPIVOT (
    metric_value FOR measure IN (row_count_avg_7 AS row_count_avg_7_v, premium_product_in_health_life AS m_premium_product_in_health_life, total_premium AS total_premium_v)
) unp
