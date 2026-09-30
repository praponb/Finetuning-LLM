SELECT id, amount, CASE WHEN status IN ('PENDING', 'SHIPPED') THEN 'Tier 1' ELSE 'Other' END AS status_group FROM orders ORDER BY amount ASC, status_group ASC;
