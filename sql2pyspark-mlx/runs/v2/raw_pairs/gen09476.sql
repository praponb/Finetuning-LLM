select a.segment, b.city from (select distinct segment from customers) a cross join (select distinct city from stores) b;
