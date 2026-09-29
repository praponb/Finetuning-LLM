WITH enriched AS (
    SELECT
        *,
        LAG(units) OVER (PARTITION BY store_id, product_id ORDER BY units, id) AS prev_units,
        LEAD(units) OVER (PARTITION BY store_id, product_id ORDER BY units, id) AS next_units,
        LAG(revenue) OVER (PARTITION BY store_id, product_id ORDER BY units, id) AS prev_revenue,
        LEAD(revenue) OVER (PARTITION BY store_id, product_id ORDER BY units, id) AS next_revenue,
        ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY units, id) AS event_seq,
        ROUND(units / SUM(units) OVER (PARTITION BY store_id), 4) AS units_share
    FROM store_sales
    WHERE units BETWEEN 32 AND 37
)
SELECT
    id,
    product_id,
    sale_date,
    revenue,
    prev_units,
    prev_revenue,
    next_revenue,
    event_seq
FROM enriched
ORDER BY id DESC
LIMIT 10
