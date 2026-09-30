select x.city, y.country from (select distinct city from customers) x cross join (select distinct country from suppliers) y order by city, country;
