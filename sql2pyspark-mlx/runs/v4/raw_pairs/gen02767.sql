SELECT s.id, COUNT(o.id) AS order_count, MAX(o.order_date) AS latest_order_date FROM stores s LEFT OUTER JOIN orders o ON o.store_id = s.id WHERE s.sqft <> 16003 GROUP BY s.id
