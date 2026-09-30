WITH store_summary AS (SELECT store_id, COUNT(*) AS cnt FROM orders GROUP BY store_id) SELECT s.id, s.store_name, ss.cnt FROM stores s LEFT JOIN store_summary ss ON s.id = ss.store_id
