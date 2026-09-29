SELECT customer_id, channel FROM orders INTERSECT SELECT customer_id, channel FROM orders WHERE discount IS NOT NULL
