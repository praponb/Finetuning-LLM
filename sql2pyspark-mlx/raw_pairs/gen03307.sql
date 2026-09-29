with flattened as (
    select
        id,
        created_at,
        coupon_codes,
        pos,
        line.qty,
        line.price
    from carts
    lateral view outer posexplode(items) e as pos, line
),
lines as (
    select
        id,
        pos,
        value
    from flattened
    lateral view explode(coupon_codes) e as value
),
cleaned as (
    select *, initcap(trim(value)) value_title
    from lines
    where value is distinct from 'WELCOME5'
),
scored as (
    select
        *,
        cume_dist() over (order by pos desc nulls last) pos_cume_dist,
        rank() over (order by pos nulls last) pos_rank,
        max(pos) over (partition by value) value_max_pos,
        rank() over w_ordered pos_rank_2,
        cume_dist() over (partition by value order by pos desc nulls first) pos_cume_dist_2
    from cleaned
    where pos >= 3
    window w_ordered as (partition by value order by pos desc nulls last)
)
select
    id,
    pos,
    value,
    value_title,
    pos_rank,
    pos_cume_dist_2
from scored;
