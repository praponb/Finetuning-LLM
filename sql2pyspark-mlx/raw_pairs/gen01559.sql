select id, city, opened_date, sqft from (select *, row_number() over (partition by city order by opened_date, id) as rnk from stores where sqft >= 10499) as r where rnk <= 2 order by sqft, city
