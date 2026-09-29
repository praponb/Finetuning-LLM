select sale_date from store_sales union all select signup_date as sale_date from customers order by sale_date;
