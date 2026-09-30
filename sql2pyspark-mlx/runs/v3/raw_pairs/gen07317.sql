SELECT units, product_id, revenue FROM store_sales WHERE product_id = 1 AND revenue < 2200 AND revenue BETWEEN 1500 AND 2600 ORDER BY revenue DESC
