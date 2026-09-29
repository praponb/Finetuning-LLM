select *, case when units >= 31 then 'High' when units >= 15 then 'Medium' else 'Low' end as units_tier, date_add(sale_date, 67) as sale_date_plus_67d from store_sales;
