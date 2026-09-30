select country, unique_countrys from (select country, count(distinct rating) as unique_countrys from suppliers group by country) as t where unique_countrys < 6 order by country;
