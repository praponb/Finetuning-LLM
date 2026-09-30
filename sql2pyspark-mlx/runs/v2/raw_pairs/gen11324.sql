select id, sqft, opened_date from stores where sqft > (select avg(sqft) from stores) order by opened_date, id limit 5
