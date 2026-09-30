select x.city, y.level from (select distinct city from customers) x cross join (select distinct level from employees) y order by city, level
