SELECT shipped_date FROM shipments WHERE cost BETWEEN 31 AND 35 UNION SELECT shipped_date FROM shipments WHERE YEAR(delivered_date) = 2024
