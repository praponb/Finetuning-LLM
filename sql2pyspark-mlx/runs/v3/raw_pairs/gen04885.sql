SELECT c.segment, SUM(CASE WHEN o.channel = 'partner' THEN o.discount ELSE 0 END) AS partner_discount
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
WHERE NOT c.is_active
GROUP BY c.segment
HAVING partner_discount >= 26
ORDER BY partner_discount DESC;
