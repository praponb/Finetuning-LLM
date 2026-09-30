with with_last as (
    select
        s.id,
        s.country,
        s.city,
        s.rating,
        top1.category
    from suppliers s
    join lateral (
            select c.category
            from products c
            where c.supplier_id = s.id
            order by c.cost desc, c.id desc
            limit 1
        ) top1 on true
)
select
    id,
    country,
    rating,
    category,
    case when category rlike '^(Books|Home)$' then 'GROUP_A' else 'GROUP_B' end as category_zone
from with_last
order by category asc;
