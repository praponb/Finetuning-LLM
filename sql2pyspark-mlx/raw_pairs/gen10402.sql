select id, carrier, shipped_date, delivered_date from shipments qualify row_number() over (partition by order_id order by cost desc nulls first, id) = 1;
