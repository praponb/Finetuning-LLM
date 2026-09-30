select id, customer_id, coalesce(status, 'N/A') as status_filled
from accounts
where status in ('CLOSED', 'OPEN', 'FROZEN');
