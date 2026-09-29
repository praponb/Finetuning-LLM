select id, region, opened_date, store_name, first_value(store_name) over (partition by region order by opened_date, id) first_store_name from stores where year(opened_date) = 2023;
