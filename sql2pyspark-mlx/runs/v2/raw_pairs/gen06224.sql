select method, amount_range
from (
    select method, max(amount) - min(amount) as amount_range
    from payments
    where method in ('card', 'paypal')
    group by method
) as sub
where amount_range >= 47
order by amount_range
