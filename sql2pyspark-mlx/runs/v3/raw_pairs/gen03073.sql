select id, country, rating from (select *, rank() over (partition by country order by rating desc) as rnk from suppliers) as t where rnk <= 5
