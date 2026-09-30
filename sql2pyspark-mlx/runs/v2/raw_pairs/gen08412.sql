select ss.units, ss.store_id, ss.sale_date, p.brand, p.stock_qty, p.product_name from store_sales ss join products p on ss.product_id = p.id
