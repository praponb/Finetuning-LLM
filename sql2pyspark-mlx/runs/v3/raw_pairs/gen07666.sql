select sale_date from store_sales union all select launch_date as sale_date from products where array_contains(tags, 'new')
