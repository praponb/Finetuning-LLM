with scored as (
    -- Running and moving aggregates
    select
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        round(amount / sum(amount) over (partition by store_id), 4) amount_share,
        round(amount / sum(amount) over (partition by store_id), 4) amount_share_2,
        row_number() over (order by amount desc, id asc) seq_no
    from orders
),
joined as (
    -- Attach customers attributes
    select a.*, c.country
    from scored a
    join customers c
        on a.customer_id = c.id
),
enriched as (
    -- Attach stores attributes
    select
        e.*,
        s.city,
        s.region,
        s.store_name
    from joined e
    left join stores s
        on e.store_id = s.id
)
-- Aggregate with conditional metrics
select
    country,
    approx_percentile(seq_no, 0.5) as seq_no_p50,
    round(sum(amount) / nullif(count(case when country = 'DE' then 1 end), 0), 2) as amount_per_de,
    count(distinct status) as distinct_status,
    sort_array(collect_set(channel)) as channel_list,
    round(sum(discount) / nullif(count(case when country = 'DE' then 1 end), 0), 2) as discount_per_de,
    count_if(region = 'North') as north_events
from enriched
group by 1;
