with kept_keys as (
    -- Set difference on keys
    select status
    from orders
    except distinct
    select status
    from orders
    where order_date <= '2024-07-01'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from orders s
    join kept_keys k on s.status = k.status
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from reconciled s
    where not exists (
            select 1
            from shipments c
            where c.order_id = s.id
              and c.status = 'IN_TRANSIT'
        )
),
attributed as (
    -- Non-equi join on a validity window
    select
        src.*,
        pr.id as promo_id,
        pr.promo_code,
        pr.discount_pct
    from not_flagged src
    join promotions pr
        on src.channel <=> pr.channel
       and src.order_date >= pr.starts_on
       and src.order_date <= date_add(pr.ends_on, 10)
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        greatest(discount, promo_id) as greatest_discount_promo_id,
        case when status in ('PENDING', 'RETURNED') then 'Core' else 'Other' end as status_group,
        coalesce(channel, 'OTHER') as channel_filled
    from attributed
    where order_date > date '2023-07-08'
      and channel is not null
)
-- Grouped metrics
select
    case when discount_pct >= 0.34 then 'PREMIUM' else 'STANDARD' end as discount_pct_tier,
    max(discount) as max_discount,
    count(distinct id) as distinct_ids,
    count_if(channel = 'partner') as partner_events
from filtered
group by case when discount_pct >= 0.34 then 'PREMIUM' else 'STANDARD' end
order by partner_events desc nulls first;
