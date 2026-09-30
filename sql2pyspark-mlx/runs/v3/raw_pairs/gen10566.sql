SELECT
    DATE_TRUNC('MONTH', stores.opened_date) AS opened_month,
    CASE WHEN stores.sqft >= 17820 THEN 'high' WHEN stores.sqft >= 6122 THEN 'mid' ELSE 'low' END AS sqft_bucket,
    SUM(store_sales.units) AS total_units,
    MIN(store_sales.sale_date) AS first_sale_date
FROM store_sales
LEFT OUTER JOIN products ON products.id = store_sales.product_id
INNER JOIN stores ON store_sales.store_id = stores.id
GROUP BY DATE_TRUNC('MONTH', stores.opened_date), CASE WHEN stores.sqft >= 17820 THEN 'high' WHEN stores.sqft >= 6122 THEN 'mid' ELSE 'low' END
ORDER BY sqft_bucket DESC, opened_month DESC
LIMIT 5
