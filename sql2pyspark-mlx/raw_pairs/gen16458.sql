WITH filtered AS (
    SELECT *, DATEDIFF(CURRENT_DATE(), sale_date) / 365.25 AS years_since_sale_date
    FROM store_sales
    WHERE sale_date >= DATE '2023-08-14'
)
SELECT
    id,
    store_id,
    product_id,
    years_since_sale_date,
    AVG(years_since_sale_date) OVER (PARTITION BY product_id, store_id ORDER BY years_since_sale_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS years_since_sale_date_avg_5_prior,
    CORR(revenue, units) OVER (PARTITION BY store_id) AS corr_revenue_units,
    RANK() OVER (PARTITION BY product_id ORDER BY years_since_sale_date DESC NULLS FIRST) AS years_since_sale_date_rank,
    LAST_VALUE(years_since_sale_date, TRUE) OVER (PARTITION BY store_id ORDER BY years_since_sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_years_since_sale_date
FROM filtered
WHERE product_id = 2;
