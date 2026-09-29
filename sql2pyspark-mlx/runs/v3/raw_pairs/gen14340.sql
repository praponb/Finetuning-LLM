select ss.revenue, ss.product_id, s.region, s.opened_date, p.supplier_id from store_sales ss inner join stores s on s.id = ss.store_id left join products p on ss.product_id = p.id;
