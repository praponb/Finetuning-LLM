SELECT id, product_id, unit_price FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price DESC, id) rn FROM order_items) ranked WHERE rn <= 3 ORDER BY product_id DESC, id;
