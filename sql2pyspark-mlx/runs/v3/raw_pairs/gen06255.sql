SELECT id, sale_date, product_id FROM store_sales WHERE NOT (revenue > 2400 AND product_id = 8 AND revenue <= 2600);
