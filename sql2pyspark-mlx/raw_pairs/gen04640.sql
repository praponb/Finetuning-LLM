select id, country, rating from (select *, rank() over (partition by country order by rating desc) as rn from suppliers where rating < 2.2) as r where rn <= 5;
