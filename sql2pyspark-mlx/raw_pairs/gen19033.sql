SELECT
    id,
    discount,
    channel,
    CASE WHEN discount >= 0.34 THEN 'High' WHEN discount >= 0.13 THEN 'Medium' ELSE 'Low' END AS discount_band,
    ROUND(discount, 0) AS discount_rounded
FROM orders
WHERE discount > 0.16 AND discount > 0.29
ORDER BY channel DESC, id DESC
LIMIT 1;
