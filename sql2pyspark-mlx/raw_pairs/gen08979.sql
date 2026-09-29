select p.stock_qty, ss.revenue, ss.units from products p left join store_sales ss on ss.product_id = p.id where ss.sale_date <= '2024-12-01' order by stock_qty desc;
