with ranked as (select id, city, opened_date, sqft, row_number() over (partition by city order by opened_date, id) as pos from stores) select id, city, opened_date, sqft from ranked where pos <= 5;
