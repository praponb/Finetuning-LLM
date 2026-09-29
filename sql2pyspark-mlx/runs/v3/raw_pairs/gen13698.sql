with Ranked as (
    select *, row_number() over (partition by channel order by shipping_fee desc, id) as pos
    from orders
    where channel in ('partner', 'store')
)
select *
from Ranked
where pos = 1
order by amount desc;
