select id, opened_date, region, upper(trim(store_name)) as store_name_normalized from stores where city = 'London' order by region;
