with with_window as (
    -- Non-equi join on a validity window
    select
        e.*,
        pr.id as promo_id,
        pr.promo_code,
        pr.discount_pct
    from orders e
    left join promotions pr
        on e.channel = pr.channel
       and e.order_date >= pr.starts_on
       and e.order_date < pr.ends_on + interval 7 days
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        sum(shipping_fee) over w_running as running_shipping_fee,
        sum(discount) over (partition by status order by shipping_fee, id, promo_id rows between unbounded preceding and current row) as running_discount,
        row_number() over (partition by channel order by promo_id desc, id, promo_id) as row_seq
    from with_window
    where status not in ('RETURNED', 'PENDING', 'SHIPPED')
    window w_running as (order by amount, id, promo_id rows between unbounded preceding and current row)
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from metrics
    qualify row_number() over (partition by status, customer_id order by running_discount nulls first, id, promo_id) = 1
),
kept_keys as (
    -- Set intersection on keys
    select status, channel
    from ranked
    intersect all
    select status, channel
    from orders
    where status = 'COMPLETED'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from ranked s
    inner join kept_keys k on s.status = k.status and s.channel = k.channel
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        coalesce(shipping_fee, 0) as shipping_fee_filled,
        coalesce(channel, 'N/A') as channel_filled
    from reconciled
    where channel is not null
      and running_shipping_fee >= 184
)
select
    id,
    customer_id,
    store_id,
    order_date,
    status,
    channel,
    amount,
    shipping_fee,
    promo_id,
    case when status rlike '^(RETURNED|SHIPPED)$' then 'GROUP_B' else 'PRIORITY' end as status_zone
from filtered
order by order_date nulls last
