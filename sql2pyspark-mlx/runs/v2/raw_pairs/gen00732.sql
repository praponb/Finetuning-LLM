select id, region, opened_date, sqft, lead(sqft, 1) over (partition by region order by opened_date, id) as next_sqft from stores;
