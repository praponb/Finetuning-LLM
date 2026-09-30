select merchant, sum(amount) as total_amount
from transactions
where amount > 1200
group by merchant
having sum(amount) >= 9000
order by merchant, total_amount;
