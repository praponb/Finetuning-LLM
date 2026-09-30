WITH latest_match AS (
    SELECT
        s.id,
        s.region,
        s.opened_date,
        s.sqft,
        latest.store_sale_sale_date,
        latest.store_sale_revenue
    FROM stores s
    LEFT JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date, c.revenue AS store_sale_revenue
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
metrics AS (
    SELECT
        *,
        LEAD(sqft) OVER (PARTITION BY region ORDER BY sqft, id) AS next_sqft,
        LEAD(store_sale_revenue) OVER (PARTITION BY region ORDER BY sqft, id) AS next_store_sale_revenue,
        LAG(store_sale_revenue) OVER (PARTITION BY region ORDER BY sqft, id) AS prev_store_sale_revenue,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft, id) AS row_seq
    FROM latest_match
    WHERE sqft >= 14192
),
remaining_keys AS (
    SELECT region
    FROM metrics
    INTERSECT
    SELECT region
    FROM stores
    WHERE region = 'Central'
),
reconciled AS (
    SELECT s.*
    FROM metrics s
    INNER JOIN remaining_keys k ON s.region = k.region
),
eligible AS (
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.customer_id IN (2, 8, 13, 14)
        )
),
with_windows AS (
    SELECT
        *,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY region), 4) AS sqft_share,
        CUME_DIST() OVER (PARTITION BY region ORDER BY next_store_sale_revenue DESC NULLS FIRST) AS next_store_sale_revenue_cume_dist
    FROM eligible
),
scored AS (
    SELECT
        *,
        AVG(sqft) OVER (PARTITION BY region) AS region_avg_sqft,
        PERCENT_RANK() OVER (ORDER BY prev_store_sale_revenue NULLS LAST) AS prev_store_sale_revenue_percent_rank,
        CUME_DIST() OVER (PARTITION BY region ORDER BY sqft DESC NULLS LAST) AS sqft_cume_dist,
        RANK() OVER (PARTITION BY region ORDER BY row_seq DESC) AS row_seq_rank
    FROM with_windows
),
filtered AS (
    SELECT
        opened_date,
        sqft,
        store_sale_sale_date,
        next_store_sale_revenue,
        prev_store_sale_revenue,
        row_seq,
        next_store_sale_revenue_cume_dist,
        prev_store_sale_revenue_percent_rank,
        sqft_cume_dist,
        row_seq_rank,
        LENGTH(region) AS region_length,
        COALESCE(prev_store_sale_revenue_percent_rank, 0) AS prev_store_sale_revenue_percent_rank_filled
    FROM scored
    WHERE opened_date >= DATE '2023-04-16'
)
SELECT
    CASE WHEN next_store_sale_revenue >= 3700 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_store_sale_revenue_tier,
    APPROX_PERCENTILE(next_store_sale_revenue, 0.9) AS next_store_sale_revenue_p90,
    ROUND(AVG(prev_store_sale_revenue_percent_rank_filled), 2) AS avg_prev_store_sale_revenue_percent_rank_filled,
    BIT_AND(sqft) AS sqft_bit_and
FROM filtered
GROUP BY next_store_sale_revenue_tier
ORDER BY next_store_sale_revenue_p90 DESC NULLS FIRST
