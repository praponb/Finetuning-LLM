select amount as value, 'payment' as source from payments where status != 'SUCCESS' union all select revenue as value, 'store_sale' as source from store_sales where month(sale_date) in (1, 10, 12)
