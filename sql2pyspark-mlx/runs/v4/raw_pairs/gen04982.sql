WITH base AS (
    SELECT
        *,
        COALESCE(price, 0) AS price_filled,
        DATE_TRUNC('WEEK', launch_date) AS launch_date_period
    FROM products
    WHERE launch_date > DATE '2024-02-24'
      AND brand IS NOT NULL
      AND category IN ('Home', 'Grocery')
)
SELECT s.*
FROM base s
WHERE NOT EXISTS (
        SELECT 1
        FROM store_sales ch
        WHERE ch.product_id = s.id
          AND ch.sale_date < s.launch_date
    );
