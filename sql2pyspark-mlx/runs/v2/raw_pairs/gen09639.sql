SELECT c.country, o.store_id FROM customers c INNER JOIN orders o ON o.customer_id = c.id WHERE o.channel <> 'web' AND UPPER(TRIM(c.name)) = 'KANYA SRISUK' ORDER BY o.store_id ASC, country ASC
