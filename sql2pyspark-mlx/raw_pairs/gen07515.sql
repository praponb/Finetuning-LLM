with latest_match as (
    select s.*, x.order_item_unit_price
    from orders s
    left join lateral (
            select c.unit_price as order_item_unit_price
            from order_items c
            where c.order_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) x on true
),
with_window as (
    select
        src.*,
        pr.id as promo_id,
        pr.promo_code,
        pr.discount_pct
    from latest_match src
    join promotions pr
        on src.channel = pr.channel
       and src.order_date >= pr.starts_on
       and src.order_date < pr.ends_on + interval '7' day
),
remaining_keys as (
    select status
    from with_window
    except
    select status
    from orders
    where amount between 1270 and 1300
),
reconciled as (
    select s.*
    from with_window s
    join remaining_keys k on s.status = k.status
),
kept as (
    select s.*
    from reconciled s
    where not exists (
            select 1
            from shipments ch
            where ch.order_id = s.id
              and year(ch.shipped_date) >= 2024
        )
),
metrics as (
    select
        *,
        sum(shipping_fee) over (partition by status) as status_sum_shipping_fee,
        cume_dist() over (partition by year(order_date), store_id order by promo_id nulls first) as promo_id_cume_dist
    from kept
),
not_flagged as (
    select s.*
    from metrics s
    where exists (
            select 1
            from order_items ch
            where ch.order_id = s.id
              and ch.unit_price >= 230
        )
),
attributed as (
    select
        s.*,
        pr.id as promo_id_2,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from not_flagged s
    left join promotions pr
        on s.channel = pr.channel
       and s.order_date between pr.starts_on and pr.ends_on
)
select
    s.customer_id,
    s.store_id,
    s.order_date,
    s.channel,
    s.amount,
    s.shipping_fee,
    s.promo_id,
    s.promo_code,
    s.discount_pct,
    case when channel rlike '^(partner|web)$' then 'CORE' when channel rlike '^(mobile|store)$' then 'PRIORITY' else 'EXTENDED' end as channel_zone,
    (select avg(i2.quantity) from inventory i2 where i2.store_id = s.store_id) as avg_inventory_quantity
from attributed s
order by promo_id, promo_code desc;
