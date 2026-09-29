SELECT a.method, b.carrier FROM (SELECT DISTINCT method FROM payments) a CROSS JOIN (SELECT DISTINCT carrier FROM shipments) b ORDER BY method, carrier
