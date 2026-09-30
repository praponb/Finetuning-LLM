select ss.revenue, p.brand, p.supplier_id, p.cost from store_sales ss inner join products p on p.id = ss.product_id where ss.units between 26 and 31;
