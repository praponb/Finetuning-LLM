with exploded as (
    select
        id,
        coupon_codes,
        li_item.product_id,
        li_item.qty,
        li_item.price,
        li_item.promo
    from (
        select
            id,
            coupon_codes,
            explode_outer(items) as li_item
        from carts
    ) e
),
flattened as (
    select promo, explode_outer(coupon_codes) as code
    from exploded
),
cleaned as (
    select
        *,
        concat(promo, ' ', code) as promo_code,
        coalesce(promo, 'N/A') as promo_filled
    from flattened
    where code is distinct from 'VIP'
      and code in ('SPRING10', 'WELCOME5', 'BUNDLE')
),
cleaned_carts as (
    select
        *,
        substring(promo, 1, 3) as promo_prefix,
        lower(code) as code_lower
    from cleaned
    where code is not null
),
filtered as (
    select
        promo,
        code,
        promo_code,
        promo_filled,
        code_lower,
        trim(promo) as clean_promo,
        coalesce(promo, 'N/A') as resolved_promo
    from cleaned_carts
    where promo is not null
)
select
    code,
    count(case when code = 'SPRING10' then 1 end) as spring10_count,
    sort_array(collect_set(promo)) as promo_list,
    count(*) filter (where code = 'SPRING10') as spring10_rows
from filtered
group by code
having count(case when code = 'SPRING10' then 1 end) < 1000
order by spring10_count desc nulls first;
