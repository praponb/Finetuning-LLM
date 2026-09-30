select id, carrier, delivered_date from (select *, rank() over (partition by order_id order by cost desc nulls first) as row_num from shipments) t where row_num = 1
