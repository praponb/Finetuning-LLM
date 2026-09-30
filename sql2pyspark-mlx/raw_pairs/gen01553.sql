WITH latest_match AS (
    SELECT
        s.*,
        top1.store_sale_sale_date,
        top1.revenue
    FROM stores s
    JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date, c.revenue
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
not_flagged AS (
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date >= s.store_sale_sale_date
        )
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY revenue DESC NULLS LAST, id) AS dedup_rank
        FROM not_flagged
    ) t
    WHERE dedup_rank = 1
),
not_flagged_stores AS (
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.store_id = s.id
              AND x.channel IN ('mobile', 'web')
        )
),
scored AS (
    SELECT
        id,
        store_name,
        region,
        opened_date,
        sqft,
        revenue,
        MAX(revenue) OVER w_running AS running_max_revenue,
        MIN(revenue) OVER w_running AS running_min_revenue,
        COUNT(revenue) OVER w_running AS running_count_revenue,
        AVG(revenue) OVER w_running AS running_avg_revenue
    FROM not_flagged_stores
    WINDOW w_running AS (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    store_name,
    opened_date,
    sqft,
    revenue,
    running_max_revenue,
    running_min_revenue,
    running_count_revenue,
    running_avg_revenue
FROM scored
