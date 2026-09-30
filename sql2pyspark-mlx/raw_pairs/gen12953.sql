with lines as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        explode_outer(coupon_codes) as tag
    from carts
),
joined as (
    -- Attach customers attributes
    select
        src.*,
        p.country,
        p.city
    from lines src
    left join customers p
        on src.customer_id = p.id
    where p.credit_limit <= 6000
),
kept_keys as (
    -- Set difference on keys
    select id, customer_id
    from joined
    except
    select id, customer_id
    from carts
    where created_at >= '2024-01-19'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from joined s
    inner join kept_keys k on s.id = k.id and s.customer_id = k.customer_id
),
cleaned as (
    -- Basic cleansing and defaults
    select *, case when tag in ('SPRING10', 'WELCOME5') then 'Priority' else 'Other' end as tag_group
    from reconciled
    where created_at > date '2025-03-01'
      and country is not distinct from 'SG'
)
select
    s.id,
    s.customer_id,
    s.created_at,
    s.tag,
    s.tag_group,
    (select sum(o2.amount) from orders o2 where o2.customer_id = s.customer_id) as sum_order_amount
from cleaned s
order by created_at;
