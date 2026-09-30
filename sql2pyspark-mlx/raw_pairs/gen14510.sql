WITH with_dims AS (
    SELECT
        a.*,
        p.discount,
        p.shipping_fee,
        p.order_date
    FROM order_items a
    LEFT JOIN orders p
        ON a.order_id = p.id
),
with_dims_order_items AS (
    SELECT s.*, p.brand
    FROM with_dims s
    LEFT JOIN products p
        ON s.product_id = p.id
),
with_dims_order_items_2 AS (
    SELECT
        e.*,
        b.discount AS order_discount,
        b.amount,
        b.channel
    FROM with_dims_order_items e
    INNER JOIN orders b
        ON e.order_id = b.id
),
matched_keys AS (
    SELECT order_id, product_id
    FROM with_dims_order_items_2
    INTERSECT
    SELECT order_id, product_id
    FROM order_items
    WHERE order_id IN (3, 5)
),
reconciled AS (
    SELECT s.*
    FROM with_dims_order_items_2 s
    INNER JOIN matched_keys k ON s.order_id = k.order_id AND s.product_id = k.product_id
)
SELECT
    DATE_TRUNC('WEEK', order_date) AS week_start,
    DATE_TRUNC('MONTH', order_date) AS activity_month,
    COUNT(*) FILTER (WHERE brand = 'Initech') AS initech_rows,
    COUNT(DISTINCT channel) AS distinct_channels,
    APPROX_PERCENTILE(unit_price, ARRAY(0.25, 0.5, 0.75)) AS unit_price_quartiles,
    SIZE(COLLECT_SET(brand)) AS num_brands,
    ROUND(SUM(quantity) / NULLIF(COUNT(CASE WHEN channel = 'store' THEN 1 END), 0), 2) AS quantity_per_store,
    ROUND(SUM(quantity) / NULLIF(COUNT(CASE WHEN brand = 'Acme' THEN 1 END), 0), 2) AS quantity_per_acme,
    DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('WEEK', order_date) ORDER BY COUNT(*) FILTER (WHERE brand = 'Initech') DESC) AS initech_rows_dense_rank
FROM reconciled
GROUP BY 1, 2;
