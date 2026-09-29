select a.country, b.city from (select distinct country from customers) a cross join (select distinct city from suppliers) b order by country, city;
