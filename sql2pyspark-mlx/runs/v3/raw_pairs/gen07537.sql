SELECT
    id,
    channel,
    status,
    discount,
    CASE WHEN amount >= 1360 THEN 'High' WHEN amount >= 740 THEN 'Medium' ELSE 'Low' END amount_level
FROM orders
ORDER BY 3
