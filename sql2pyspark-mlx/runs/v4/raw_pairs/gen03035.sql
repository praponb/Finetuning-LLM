select s.device, c.segment
from sessions s
full outer join customers c on c.id = s.customer_id
order by c.segment desc, device;
