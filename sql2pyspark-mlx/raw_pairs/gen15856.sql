with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        coupon_codes,
        pos,
        li_item.product_id,
        li_item.qty
    from carts
    lateral view outer posexplode(items) x as pos, li_item
),
flattened_carts as (
    -- Unnest the array into one row per element
    select pos, code
    from flattened
    lateral view outer explode(coupon_codes) li as code
),
cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        case when code = 'VIP' then 1 else 0 end as is_vip,
        lower(code) as code_lower
    from flattened_carts
    where pos between 1 and 3
),
enriched as (
    -- Running and moving aggregates
    select *, max(pos) over w_grp as code_max_pos
    from cleaned
    window w_grp as (partition by code)
),
base as (
    -- Keep recent, valid rows
    select
        *,
        upper(code) as code_upper,
        coalesce(code, 'DIRECT') as code_clean
    from enriched
    where code is not null
      and code_max_pos > 3
),
scored as (
    -- Running and moving aggregates
    select
        pos,
        code,
        code_lower,
        code_max_pos,
        code_clean,
        min(pos) over (partition by code) as code_min_pos,
        percent_rank() over (partition by code order by code_max_pos desc) as code_max_pos_percent_rank,
        percent_rank() over (partition by code order by pos asc nulls last) as pos_percent_rank,
        cume_dist() over (partition by code order by code_max_pos asc) as code_max_pos_cume_dist,
        rank() over (order by code_max_pos asc nulls last) as code_max_pos_rank
    from base
)
select
    pos,
    code,
    code_lower,
    code_max_pos,
    code_clean,
    code_max_pos_percent_rank,
    code_max_pos_cume_dist,
    case when code rlike '^(VIP|WELCOME5)$' then 'GROUP_B' else 'OTHER_ZONE' end as code_zone
from scored
order by code_max_pos_cume_dist desc;
