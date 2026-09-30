SELECT CASE WHEN stores.sqft >= 16609 THEN 'high' WHEN stores.sqft >= 6669 THEN 'mid' ELSE 'low' END AS sqft_bucket, MIN(store_sales.revenue) AS min_revenue
FROM store_sales
RIGHT OUTER JOIN stores ON store_sales.store_id = stores.id
GROUP BY CASE WHEN stores.sqft >= 16609 THEN 'high' WHEN stores.sqft >= 6669 THEN 'mid' ELSE 'low' END
ORDER BY min_revenue DESC;
