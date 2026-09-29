SELECT COUNT(*) AS total_count
FROM order_items
WHERE unit_price < 140
ORDER BY total_count DESC
