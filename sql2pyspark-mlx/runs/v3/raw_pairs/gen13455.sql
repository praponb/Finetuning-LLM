select id, region, sqft, row_number() over (partition by region order by sqft desc, id) as row_num from stores
