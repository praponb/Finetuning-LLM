with numbered as (select *, dense_rank() over (partition by country order by rating) rn from suppliers) select id, country, rating from numbered where rn <= 3 order by rating desc
