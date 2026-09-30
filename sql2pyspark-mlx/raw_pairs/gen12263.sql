select id, status, case when cost >= 83 then 'High' when cost >= 25 then 'Medium' else 'Low' end as cost_level
from shipments
where order_id is null
