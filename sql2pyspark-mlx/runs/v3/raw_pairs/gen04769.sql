select id, opened_date, region, sqft from stores where opened_date = (select min(opened_date) from stores) order by 1 desc
