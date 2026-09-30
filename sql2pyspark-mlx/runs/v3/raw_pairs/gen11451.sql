with ranked as (select id, region, opened_date, city, row_number() over (partition by region order by opened_date desc, id) as rnk from stores) select * from ranked where rnk <= 5 order by id desc
