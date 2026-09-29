select status, total_amount
from (
    select status, sum(amount) as total_amount
    from payments
    group by status
) as t
where total_amount > 4400
order by 1, total_amount desc;
