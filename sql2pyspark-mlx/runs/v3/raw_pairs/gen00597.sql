WITH base AS (
    SELECT *
    FROM store_sales
    WHERE units != 13
),
agg AS (
    SELECT
        store_id,
        product_id,
        COUNT(revenue) revenue_count,
        COUNT(*) store_sale_count
    FROM base
    GROUP BY store_id, product_id
)
SELECT *
FROM agg
ORDER BY store_id;
