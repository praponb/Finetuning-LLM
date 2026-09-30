select * from (select *, dense_rank() over (partition by country order by rating desc) as rn from suppliers) as t where rn = 1 order by city;
