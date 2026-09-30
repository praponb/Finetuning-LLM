SELECT shipped_date FROM shipments UNION SELECT opened_date AS shipped_date FROM stores ORDER BY shipped_date DESC
