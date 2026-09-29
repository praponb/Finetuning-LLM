with joined as (
    -- Attach stores attributes
    select src.*, s.store_name
    from orders src
    join stores s
        on src.store_id = s.id
),
latest as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by customer_id, channel order by amount desc nulls first, id) as rn
    from joined
    qualify rn = 1
),
latest_orders as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by customer_id, channel order by amount desc nulls last, id) as dedup_rank
        from latest
    ) ranked
    where dedup_rank = 1
),
metrics as (
    -- Ranking and gap features
    select
        *,
        avg(shipping_fee) over (partition by channel order by discount, id rows between 3 preceding and 1 preceding) shipping_fee_avg_3_prior,
        ntile(10) over w_hist discount_bucket,
        sum(discount) over (partition by status order by dedup_rank, id rows between unbounded preceding and current row) running_discount
    from latest_orders
    window w_hist as (partition by store_id, status order by discount, id)
)
-- Aggregate with conditional metrics
select
    channel,
    case when amount >= 1680 then 'PREMIUM' else 'STANDARD' end as amount_tier,
    round(sum(running_discount) / nullif(count(case when channel = 'store' then 1 end), 0), 2) as running_discount_per_store,
    round(sum(shipping_fee_avg_3_prior) / nullif(count(case when channel = 'partner' then 1 end), 0), 2) as shipping_fee_avg_3_prior_per_partner,
    size(collect_set(channel)) as num_channels,
    dense_rank() over (partition by channel order by size(collect_set(channel)) desc) as num_channels_dense_rank
from metrics
group by channel, case when amount >= 1680 then 'PREMIUM' else 'STANDARD' end;
