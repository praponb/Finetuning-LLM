select id, city, opened_date, sqft, lead(sqft, 2) over (partition by city order by opened_date, id) as next_sqft from stores
