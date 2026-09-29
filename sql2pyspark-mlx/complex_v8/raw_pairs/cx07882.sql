WITH valid AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        status,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        QUARTER(end_date) AS end_date_quarter
    FROM policies
    WHERE product = 'TRAVEL'
      AND status = 'EXPIRED'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(holder_id) OVER w_end_date_quarter_status) AS distinct_holder_id_in_end_date_quarter_status,
        ROUND(AVG(premium_bucket) OVER (PARTITION BY status ORDER BY end_date_quarter ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS premium_bucket_avg_2,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY end_date_quarter, status)) AS distinct_product_in_end_date_quarter_status,
        FIRST_VALUE(premium) OVER (PARTITION BY status ORDER BY end_date_quarter ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_premium,
        MIN(premium_bucket) OVER (PARTITION BY end_date_quarter, status) AS end_date_quarter_status_min_premium_bucket,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status ORDER BY end_date_quarter ASC, id), 3) AS end_date_quarter_percent_rank
    FROM valid
    WINDOW w_end_date_quarter_status AS (PARTITION BY end_date_quarter, status)
),
enriched_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.status,
        s.premium_bucket,
        s.end_date_quarter,
        s.distinct_holder_id_in_end_date_quarter_status,
        s.premium_bucket_avg_2,
        s.distinct_product_in_end_date_quarter_status,
        s.first_premium,
        s.end_date_quarter_status_min_premium_bucket,
        s.end_date_quarter_percent_rank,
        p.region AS holder_region,
        p.risk_score AS holder_risk_score,
        p.holder_name
    FROM windowed s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
product_stats AS (
    -- Avg premium_bucket_avg_2 per product
    SELECT product, AVG(premium_bucket_avg_2) AS avg_premium_bucket_avg_2
    FROM enriched_holder
    GROUP BY product
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_premium_bucket_avg_2,
        ROUND(s.premium_bucket_avg_2 - g.avg_premium_bucket_avg_2, 2) AS premium_bucket_avg_2_minus_avg
    FROM enriched_holder s
    JOIN product_stats g ON g.product = s.product
    WHERE s.premium_bucket_avg_2 > g.avg_premium_bucket_avg_2
)
-- Subtotals with GROUPING SETS
SELECT
    distinct_product_in_end_date_quarter_status,
    holder_id,
    GROUPING(distinct_product_in_end_date_quarter_status) AS is_total_distinct_product_in_end_date_quarter_st,
    ROUND(STDDEV(premium_bucket_avg_2_minus_avg), 2) AS stddev_premium_bucket_avg_2_minus_avg
FROM above_average
GROUP BY GROUPING SETS ((distinct_product_in_end_date_quarter_status, holder_id), (distinct_product_in_end_date_quarter_status), ())
ORDER BY distinct_product_in_end_date_quarter_status NULLS LAST, holder_id NULLS LAST
