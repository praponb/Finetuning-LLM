with top_stores as (select id, city, opened_date, row_number() over (partition by city order by opened_date desc, id) as rnk from stores) select id, city, opened_date from top_stores where rnk <= 3;
