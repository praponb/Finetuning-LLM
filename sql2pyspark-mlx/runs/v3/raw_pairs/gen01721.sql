select oi.id, oi.product_id, p.price, p.stock_qty from order_items oi left join products p on oi.product_id = p.id where oi.quantity > 8 order by p.stock_qty desc;
