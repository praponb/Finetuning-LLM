with matched_keys as (
    -- Set intersection on keys
    select customer_id
    from subscriptions
    intersect all
    select customer_id
    from subscriptions
    where customer_id is null
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from subscriptions s
    join matched_keys k on s.customer_id = k.customer_id
),
with_dims as (
    -- Attach customers attributes
    select
        a.*,
        b.city,
        b.signup_date,
        b.segment
    from reconciled a
    left join customers b
        on a.customer_id <=> b.id
    where b.city in ('Bangkok', 'London')
),
enriched as (
    -- Attach customers attributes
    select
        src.*,
        d.email,
        d.segment as customer_segment,
        d.name
    from with_dims src
    left join customers d
        on src.customer_id = d.id
),
scored as (
    -- Ranking and gap features
    select
        id,
        plan,
        status,
        city,
        segment,
        customer_segment,
        name,
        round(monthly_fee / sum(monthly_fee) over (partition by city), 4) as monthly_fee_share,
        round(monthly_fee / sum(monthly_fee) over (partition by city, plan), 4) as monthly_fee_share_2,
        min(monthly_fee) over (partition by customer_id, plan) as customer_id_min_monthly_fee
    from enriched
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        city,
        customer_segment,
        monthly_fee_share,
        monthly_fee_share_2,
        customer_id_min_monthly_fee,
        rank() over (order by customer_id_min_monthly_fee nulls last) as customer_id_min_monthly_fee_rank,
        cume_dist() over (order by monthly_fee_share) as monthly_fee_share_cume_dist,
        avg(monthly_fee_share) over (partition by segment) as segment_avg_monthly_fee_share
    from scored
),
base as (
    -- Basic cleansing and defaults
    select
        *,
        case when customer_id_min_monthly_fee_rank >= 7 then 'High' when customer_id_min_monthly_fee_rank >= 3 then 'Medium' else 'Low' end as customer_id_min_monthly_fee_rank_tier,
        coalesce(customer_segment, 'NONE') as customer_segment_clean
    from with_windows
    where city is not null
      and customer_id_min_monthly_fee is not null
),
cleaned as (
    -- Keep recent, valid rows
    select *, monthly_fee_share_cume_dist + segment_avg_monthly_fee_share as monthly_fee_share_cume_dist_plus_segment_avg_monthly_fee_share
    from base
    where city is distinct from 'Chiang Mai'
)
-- Aggregate with conditional metrics
select
    case when customer_id_min_monthly_fee_rank >= 7 then 'HIGH' else 'LOW' end as customer_id_min_monthly_fee_rank_tier,
    avg(monthly_fee_share) as avg_monthly_fee_share,
    sum(case when customer_segment = 'Corporate' then segment_avg_monthly_fee_share else 0 end) as corporate_segment_avg_monthly_fee_share,
    round(sum(customer_id_min_monthly_fee_rank) / nullif(count(case when city = 'Chiang Mai' then 1 end), 0), 2) as customer_id_min_monthly_fee_rank_per_chiang_mai,
    approx_percentile(customer_id_min_monthly_fee_rank, 0.5) as customer_id_min_monthly_fee_rank_p50,
    round(sum(monthly_fee_share) / nullif(count(case when city = 'New York' then 1 end), 0), 2) as monthly_fee_share_per_new_york
from cleaned
group by case when customer_id_min_monthly_fee_rank >= 7 then 'HIGH' else 'LOW' end
order by customer_id_min_monthly_fee_rank_tier
