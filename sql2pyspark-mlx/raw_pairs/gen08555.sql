SELECT page
FROM web_events
INTERSECT
SELECT brand
FROM products
ORDER BY page
