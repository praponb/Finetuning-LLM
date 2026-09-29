select id, city, opened_date, rank() over (partition by city order by opened_date desc) rnk from stores order by id, rnk;
