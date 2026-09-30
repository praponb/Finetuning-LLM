select oi.unit_price, p.brand, p.supplier_id from order_items oi left join products p on oi.product_id = p.id;
