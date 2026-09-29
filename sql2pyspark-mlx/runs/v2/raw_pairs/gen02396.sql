select * from (select *, row_number() over (partition by order_id order by shipped_date, id) as pos from shipments) as t where pos <= 5 order by shipped_date desc
