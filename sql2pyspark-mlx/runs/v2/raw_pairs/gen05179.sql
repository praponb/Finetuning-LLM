SELECT product_id, COUNT(*) cnt FROM order_items GROUP BY product_id ORDER BY cnt DESC
