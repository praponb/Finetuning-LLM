SELECT ss.id, ss.revenue, s.opened_date FROM store_sales ss LEFT OUTER JOIN stores s ON ss.store_id = s.id WHERE s.sqft BETWEEN 5967 AND 15352 AND MONTH(s.opened_date) IN (4, 8, 12)
