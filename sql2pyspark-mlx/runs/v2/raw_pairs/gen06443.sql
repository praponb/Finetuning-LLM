select id, amount, coalesce(amount, 0) as amount_filled
from payments
where method <> 'cash'
order by amount desc
