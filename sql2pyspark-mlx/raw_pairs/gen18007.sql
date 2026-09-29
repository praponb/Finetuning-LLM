select a.country, b.region from (select distinct country from suppliers) a cross join (select distinct region from stores) b;
