WITH valid_store_sales AS (
    SELECT *
    FROM store_sales
    WHERE units != 14
),
SaleYearSummary AS (
    SELECT
        YEAR(sale_date) AS sale_year,
        product_id,
        COUNT(DISTINCT units) AS unique_products,
        COUNT(*) AS cnt
    FROM valid_store_sales
    GROUP BY YEAR(sale_date), product_id
)
SELECT sale_year, product_id, cnt
FROM SaleYearSummary
WHERE cnt >= 7
ORDER BY sale_year;
