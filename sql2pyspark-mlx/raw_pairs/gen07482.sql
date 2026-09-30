with exploded as (
    select
        id,
        created_at,
        code
    from carts
    lateral view outer explode(coupon_codes) li as code
),
base as (
    select *, trim(code) as clean_code
    from exploded
    where code is not null
      and code = 'SPRING10'
),
base_carts as (
    select
        *,
        case when code in ('SPRING10', 'VIP') then 'Tier 1' else 'Other' end as code_group,
        length(code) as code_length,
        coalesce(code, 'OTHER') as code_clean
    from base
    where created_at > timestamp '2025-03-01 12:00:00'
),
scored as (
    select
        *,
        avg(code_length) over (partition by month(created_at), code order by cast(created_at as bigint) range between 1800 preceding and current row) as code_length_avg_30m,
        dense_rank() over (partition by code order by code_length desc) as code_length_dense_rank,
        sum(code_length) over (partition by code) as code_sum_code_length,
        cume_dist() over (partition by code order by code_length nulls last) as code_length_cume_dist,
        max(code_length) over (order by cast(created_at as bigint) range between 3600 preceding and current row) as code_length_max_1h
    from base_carts
    where code in ('WELCOME5', 'VIP')
)
select
    id,
    created_at,
    code,
    clean_code,
    code_group,
    code_length,
    code_clean,
    code_length_avg_30m,
    code_length_dense_rank
from scored
order by code_length_dense_rank desc nulls last;
