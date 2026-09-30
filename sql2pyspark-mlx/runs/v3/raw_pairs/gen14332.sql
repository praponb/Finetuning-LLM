WITH base AS (
    SELECT *, quantity * unit_price AS line_total
    FROM order_items
    WHERE unit_price BETWEEN 150 AND 250
),
agg AS (
    SELECT order_id, CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END AS quantity_bucket, COUNT(*) AS total_count
    FROM base
    GROUP BY order_id, CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM agg
ORDER BY 2 DESC, order_id
LIMIT 10;
