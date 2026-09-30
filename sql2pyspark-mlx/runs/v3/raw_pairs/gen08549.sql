select id, region, opened_date, sqft, lead(sqft, 1) over (partition by region order by opened_date, id) as next_sqft from stores where sqft between 13752 and 14217 order by sqft desc, opened_date
