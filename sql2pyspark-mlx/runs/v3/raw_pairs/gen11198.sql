select id, channel, amount
from (
    select *, row_number() over (partition by channel order by amount desc, id) as rn
    from orders
    where status = 'RETURNED'
) as r
where rn <= 3
