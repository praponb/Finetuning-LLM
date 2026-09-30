SELECT YEAR(s.opened_date) AS opened_year, COUNT(o.amount) AS amount_count FROM orders o LEFT OUTER JOIN stores s ON s.id = o.store_id WHERE s.sqft > 7943 GROUP BY YEAR(s.opened_date)
