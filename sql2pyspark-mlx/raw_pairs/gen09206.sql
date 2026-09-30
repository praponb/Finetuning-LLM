with exploded as (
    select
        id,
        customer_id,
        created_at,
        line.product_id,
        line.qty,
        line.price,
        line.promo
    from carts
    lateral view outer explode(items) e as line
),
metrics as (
    select *, round(price / sum(price) over (partition by date_trunc('YEAR', created_at), customer_id), 4) as price_share
    from exploded
),
base as (
    select *, round(price * 100, 2) as adjusted_price
    from metrics
    where promo is not distinct from 'VIP'
      and qty <> 5
),
joined as (
    select
        src.customer_id,
        src.created_at,
        src.qty,
        src.promo,
        src.price_share,
        src.adjusted_price,
        d.name,
        d.country,
        d.credit_limit
    from base src
    left join customers d
        on src.customer_id = d.id
    where not d.is_active
),
by_type as (
    select *
    from (
        select
            promo,
            country,
            qty
        from joined
    ) src
    pivot (
        avg(qty) as total
        for country in ('TH', 'JP')
    )
)
select
    promo,
    TH,
    JP,
    case when promo rlike '^(WELCOME5|BUNDLE)$' then 'GROUP_A' when promo rlike '^(SPRING10|VIP)$' then 'EXTENDED' else 'PRIORITY' end as promo_zone
from by_type
order by TH;
