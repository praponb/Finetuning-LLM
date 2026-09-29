select id, city, rating from (select *, dense_rank() over (partition by city order by rating desc) as pos from suppliers) as t where pos <= 3
