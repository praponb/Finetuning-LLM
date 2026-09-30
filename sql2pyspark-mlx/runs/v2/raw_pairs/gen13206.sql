select id, amount
from payments
where hour(paid_at) between 7 and 14;
