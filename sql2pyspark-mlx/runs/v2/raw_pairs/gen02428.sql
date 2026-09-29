select p.id, p.cost, p.category from products p where p.supplier_id is not null and not exists (select 1 from store_sales ss where ss.product_id = p.id and ss.revenue > 2800)
