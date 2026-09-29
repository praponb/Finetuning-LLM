select * from (select id, country, signup_date, segment, rank() over (partition by country order by signup_date desc) as pos from customers) as t where pos = 1 order by pos desc
