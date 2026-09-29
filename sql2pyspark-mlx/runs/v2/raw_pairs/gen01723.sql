SELECT COUNT(price) AS price_count, ROUND((100 * SUM(CASE WHEN brand = 'Umbrella' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_umbrella, COUNT(DISTINCT stock_qty) AS unique_brands
FROM products
ORDER BY pct_umbrella DESC;
