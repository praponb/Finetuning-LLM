select opened_date, 'store' as source from stores union all select sale_date as opened_date, 'store_sale' as source from store_sales
