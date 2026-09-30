select id, region, opened_date, store_name, lower(city) as city_lower from stores where region in ('West', 'South') and city = 'Bangkok' and sqft is not null order by id desc
