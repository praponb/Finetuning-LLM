WITH Src AS (
    SELECT *, LAST_DAY(launch_date) AS launch_date_month_end
    FROM products
    WHERE category NOT IN ('Sports', 'Grocery', 'Fashion') AND product_name LIKE '%Sho%' AND supplier_id IN (3, 8, 36)
),
summary AS (
    SELECT
        brand,
        category,
        AVG(price) AS avg_price,
        COUNT(DISTINCT price) AS unique_categorys
    FROM Src
    GROUP BY brand, category
)
SELECT *
FROM summary
WHERE avg_price > 530;
