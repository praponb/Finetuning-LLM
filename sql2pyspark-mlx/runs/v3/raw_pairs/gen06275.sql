SELECT city, 'supplier' AS source FROM suppliers WHERE country IN ('JP', 'SG', 'US') UNION ALL SELECT city, 'store' AS source FROM stores ORDER BY source
