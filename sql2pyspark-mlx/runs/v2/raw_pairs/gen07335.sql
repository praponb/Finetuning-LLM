select id, region, opened_date, store_name, first_value(store_name) over (partition by region order by opened_date, id) as first_store_name from stores order by region desc
