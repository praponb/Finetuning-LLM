SELECT
    id,
    channel,
    discount,
    CASE WHEN status IN ('PENDING', 'SHIPPED') THEN 'Tier 1' ELSE 'Other' END AS status_group,
    CASE WHEN discount >= 0.3 THEN 'High' WHEN discount >= 0.16 THEN 'Medium' ELSE 'Low' END AS discount_level
FROM orders
WHERE discount >= 0.15 AND status = 'RETURNED'
ORDER BY channel
