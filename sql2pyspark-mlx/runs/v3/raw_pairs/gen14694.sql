select p.stock_qty, p.supplier_id, oi.order_id, oi.unit_price from products p left outer join order_items oi on p.id = oi.product_id
