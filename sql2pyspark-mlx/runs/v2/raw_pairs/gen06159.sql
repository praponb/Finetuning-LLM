select id, amount, rank() over (order by amount desc) as amount_rank
from transactions
where merchant in ('Amazon', '7-Eleven', 'Shell');
