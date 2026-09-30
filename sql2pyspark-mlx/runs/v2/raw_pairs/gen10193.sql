select p.category, max(ss.sale_date) as latest_sale_date from store_sales ss join products p on ss.product_id = p.id group by p.category order by latest_sale_date desc, p.category
