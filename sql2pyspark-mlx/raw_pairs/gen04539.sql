select * from (select *, rank() over (partition by status order by shipped_date) as pos from shipments) as t where pos <= 3;
