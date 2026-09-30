SELECT customer_id, store_id FROM orders EXCEPT DISTINCT SELECT customer_id, store_id FROM orders WHERE discount IS NOT NULL;
