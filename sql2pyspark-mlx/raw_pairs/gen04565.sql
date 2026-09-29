select *
from shipments
qualify row_number() over (partition by order_id, status order by delivered_date asc, id asc) = 1;
