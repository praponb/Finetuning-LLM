select *, row_number() over (partition by carrier, status order by cost desc nulls last, id) as row_num from shipments qualify row_num = 1
