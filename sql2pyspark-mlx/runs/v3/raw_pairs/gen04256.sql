SELECT CASE WHEN suppliers.city IN ('London', 'Bangkok') THEN 'core' ELSE 'other' END AS city_group, ROUND(STDDEV(products.price), 2) AS stddev_price
FROM products
INNER JOIN suppliers ON products.supplier_id = suppliers.id
GROUP BY CASE WHEN suppliers.city IN ('London', 'Bangkok') THEN 'core' ELSE 'other' END
