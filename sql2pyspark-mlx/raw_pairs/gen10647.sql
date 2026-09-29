select p.id, p.category, count(ss.id) as store_sale_count from products p left outer join store_sales ss on p.id = ss.product_id group by p.id, p.category;
