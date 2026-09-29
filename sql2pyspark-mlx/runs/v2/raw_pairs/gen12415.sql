SELECT product_id, SUM(quantity * unit_price) revenue FROM order_items GROUP BY 1;
