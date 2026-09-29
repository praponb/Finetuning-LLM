with with_window as (
    select
        e.*,
        pr.id as promo_id,
        pr.promo_code,
        pr.discount_pct
    from orders e
    inner join promotions pr
        on e.channel <=> pr.channel
       and e.order_date >= pr.starts_on
       and e.order_date < pr.ends_on + interval 30 days
),
scored as (
    select
        *,
        corr(amount, shipping_fee) over (partition by status, customer_id) as corr_amount_shipping_fee,
        corr(discount, amount) over (partition by channel) as corr_discount_amount,
        sum(shipping_fee) over (partition by channel order by promo_id, id, promo_id rows between unbounded preceding and current row) as cumulative_shipping_fee,
        ntile(10) over (partition by status order by promo_id, id, promo_id) as promo_id_bucket
    from with_window
),
with_windows as (
    select
        *,
        cume_dist() over (partition by store_id, channel order by promo_id nulls first) as promo_id_cume_dist,
        last_value(channel, true) over (partition by channel order by promo_id, id, promo_id rows between unbounded preceding and current row) as last_known_channel
    from scored
),
matched_keys as (
    select store_id
    from with_windows
    intersect all
    select store_id
    from orders
    where channel is not null
),
reconciled as (
    select s.*
    from with_windows s
    inner join matched_keys k on s.store_id = k.store_id
)
select
    store_id,
    count_if(channel = 'partner') as partner_events,
    percentile_approx(shipping_fee, 0.5) as shipping_fee_p50,
    round(sum(corr_discount_amount) / nullif(count(case when promo_code = 'VIP' then 1 end), 0), 2) as corr_discount_amount_per_vip,
    percentile_approx(cumulative_shipping_fee, 0.95) as cumulative_shipping_fee_p95,
    size(collect_set(channel)) as num_channels,
    round(avg(promo_id), 2) as avg_promo_id
from reconciled
group by store_id
order by cumulative_shipping_fee_p95 desc;
