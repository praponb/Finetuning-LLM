select id, category, cost
from (
    select *, dense_rank() over (partition by category order by cost desc) as pos
    from products
    where category in ('Books', 'Sports', 'Electronics')
) as t
where pos <= 3
order by 2, id desc;
