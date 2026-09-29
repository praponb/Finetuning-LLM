select a.segment, b.country from (select distinct segment from customers) a cross join (select distinct country from suppliers) b order by segment, country;
