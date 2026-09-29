SELECT brand, SUM(price) AS total_price, ROUND(SUM(price) * 100.0 / SUM(SUM(price)) OVER (), 2) AS pct_of_total
FROM products
WHERE ARRAY_CONTAINS(tags, 'eco')
GROUP BY brand
