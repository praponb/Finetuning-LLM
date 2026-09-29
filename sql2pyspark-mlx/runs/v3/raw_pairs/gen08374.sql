with RankedCustomers as (
    select *, row_number() over (partition by segment order by credit_limit, id) as pos
    from customers
)
select *
from RankedCustomers
where pos <= 3
