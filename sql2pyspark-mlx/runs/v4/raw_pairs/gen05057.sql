select a.city, b.region from (select distinct city from suppliers) a cross join (select distinct region from stores) b order by city, region
