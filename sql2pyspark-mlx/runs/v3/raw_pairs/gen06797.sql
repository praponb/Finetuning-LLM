select method, status, count(amount) as amount_count
from payments
group by method, status;
