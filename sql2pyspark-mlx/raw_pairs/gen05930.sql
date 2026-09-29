select a.city, b.region from (select distinct city from customers) a cross join (select distinct region from stores) b;
