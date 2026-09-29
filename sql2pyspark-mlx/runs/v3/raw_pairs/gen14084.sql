select x.segment, y.device from (select distinct segment from customers) x cross join (select distinct device from sessions) y
