select opened_date, lower(store_name) as store_name_lower from stores where region in ('West', 'North') and store_name is not null;
