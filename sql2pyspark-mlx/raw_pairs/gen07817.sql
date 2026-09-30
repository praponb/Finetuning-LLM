select x.country, y.browser from (select distinct country from customers) x cross join (select distinct browser from sessions) y;
