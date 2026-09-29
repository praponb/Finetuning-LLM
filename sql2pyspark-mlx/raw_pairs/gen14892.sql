select id, city, region, sqft from (select *, row_number() over (partition by region order by opened_date desc, id) as rn from stores) x where rn = 1
