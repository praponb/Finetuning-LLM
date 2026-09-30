WITH valid_store_sales AS (
    SELECT *, ADD_MONTHS(sale_date, 6) AS sale_date_plus_months
    FROM store_sales
    WHERE units BETWEEN 18 AND 34
),
agg AS (
    SELECT product_id, ROUND(STDDEV(units), 2) AS stddev_units
    FROM valid_store_sales
    GROUP BY product_id
)
SELECT product_id, stddev_units
FROM agg
WHERE stddev_units > 64
ORDER BY product_id
LIMIT 10;
