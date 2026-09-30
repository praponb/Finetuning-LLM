select p.id, p.brand, p.supplier_id from products p where p.launch_date between '2023-08-01' and '2025-03-01' and p.id in (select product_id from store_sales);
