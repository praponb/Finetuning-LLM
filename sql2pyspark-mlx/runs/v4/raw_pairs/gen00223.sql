WITH filtered AS (
    SELECT *, DATE_TRUNC('week', sale_date) AS sale_date_period
    FROM store_sales
    WHERE MONTH(sale_date) IN (7, 8, 12)
)
SELECT
    *,
    AVG(units) OVER (PARTITION BY store_id, product_id ORDER BY revenue, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS units_avg_5_prior,
    ROW_NUMBER() OVER (ORDER BY sale_date, id) AS seq_no
FROM filtered;
