with Numbered as (select *, row_number() over (partition by status order by cost, id) as pos from shipments) select * from Numbered where pos <= 3;
