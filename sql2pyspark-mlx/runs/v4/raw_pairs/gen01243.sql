select * from (select *, rank() over (partition by region order by opened_date desc) rnk from stores) t where rnk <= 2 order by city;
