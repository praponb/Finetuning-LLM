select id, opened_date, sqft, sum(sqft) over (order by opened_date, id) as running_sqft from stores
