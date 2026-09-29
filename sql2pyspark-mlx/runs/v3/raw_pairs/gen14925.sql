select id, region, sqft from (select *, row_number() over (partition by region order by sqft desc, id) as rnk from stores where sqft = 14819) as ranked where rnk <= 2 order by sqft desc
