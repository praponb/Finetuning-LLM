WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY city, region ORDER BY sqft DESC NULLS FIRST) AS dedup_rank
    FROM stores
    QUALIFY dedup_rank = 1
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.sale_date,
        top1.store_sale_units
    FROM ranked s
    JOIN LATERAL (
            SELECT c.sale_date, c.units AS store_sale_units
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        store_name,
        city,
        dedup_rank,
        store_sale_units,
        LAG(store_sale_units, 1) OVER (PARTITION BY YEAR(sale_date), region ORDER BY opened_date ASC, id ASC) AS prev_store_sale_units,
        ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY opened_date DESC, id ASC) AS event_seq,
        SUM(sqft) OVER (PARTITION BY region ORDER BY store_sale_units ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft,
        STDDEV_SAMP(sqft) OVER (PARTITION BY MONTH(opened_date), region) AS opened_date_month_stddev_samp_sqft
    FROM latest_match
    WHERE opened_date >= '2025-10-18'
),
with_last AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.dedup_rank,
        s.store_sale_units,
        s.prev_store_sale_units,
        s.event_seq,
        s.running_sqft,
        x.units,
        x.store_sale_revenue
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.units, c.revenue AS store_sale_revenue
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_latest AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.prev_store_sale_units,
        s.units,
        x.order_order_date,
        x.amount
    FROM with_last s
    JOIN LATERAL (
            SELECT c.order_date AS order_order_date, c.amount
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(amount) OVER (ORDER BY amount ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS amount_avg_3_prior,
        LAG(units, 1, 0) OVER w_recent AS prev_units
    FROM with_latest
    WINDOW w_recent AS (ORDER BY amount ASC, id ASC)
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.store_id = 3
        )
)
-- Multi-level aggregation
SELECT
    CASE WHEN amount_avg_3_prior >= 1412 THEN 'LARGE' ELSE 'SMALL' END AS amount_avg_3_prior_tier,
    COUNT(*) AS row_count,
    ROUND(AVG(units), 2) AS avg_units
FROM not_flagged
GROUP BY amount_avg_3_prior_tier
ORDER BY row_count DESC;
