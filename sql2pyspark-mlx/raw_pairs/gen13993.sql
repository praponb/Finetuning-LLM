select ss.id, ss.revenue, ss.store_id, p.cost, p.product_name from store_sales ss left join products p on ss.product_id = p.id where p.brand = 'Stark';
