select *, initcap(trim(store_name)) as store_name_title, trim(city) as clean_city from stores where store_name is null;
