select x.city, y.device from (select distinct city from suppliers) x cross join (select distinct device from sessions) y order by city, device
