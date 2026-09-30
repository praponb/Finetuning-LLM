select s.city, ss.sale_date from stores s inner join store_sales ss on s.id = ss.store_id where ss.revenue < 2300
