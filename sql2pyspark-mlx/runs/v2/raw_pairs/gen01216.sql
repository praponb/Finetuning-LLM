select method, status, min(amount) min_amount
from payments
where hour(paid_at) between 12 and 20
group by method, status
having min_amount > 1350;
