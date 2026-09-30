select *
from (
    select *, row_number() over (partition by country order by credit_limit, id) as pos
    from customers
) as t
where pos = 1;
