select s.rating, s.supplier_name, s.city, p.product_name, p.stock_qty from suppliers s left join products p on s.id = p.supplier_id where p.price < 360
