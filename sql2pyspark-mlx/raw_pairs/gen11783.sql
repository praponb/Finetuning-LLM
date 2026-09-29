select id, sqft, city, region from stores where sqft >= (select avg(sqft) from stores where opened_date >= '2024-01-01') order by id desc
