with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        status,
        datediff(current_date(), shipped_date) as days_since_shipped_date,
        coalesce(carrier, 'DIRECT') as carrier_filled
    from shipments
    where carrier is not distinct from 'FedEx'
      and shipped_date >= to_date('2023-02-11')
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by status order by delivered_date desc, id) as dedup_rank
    from cleaned
    qualify dedup_rank = 1
),
remaining_keys as (
    -- Set difference on keys
    select order_id
    from latest
    except all
    select order_id
    from shipments
    where shipped_date >= '2025-05-01'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from latest s
    inner join remaining_keys k on s.order_id = k.order_id
),
base as (
    -- Basic cleansing and defaults
    select
        *,
        datediff(current_date(), shipped_date) as days_since_shipped_date_2,
        case when status = 'LOST' then 1 else 0 end as is_lost
    from verified
    where shipped_date >= date '2023-04-06'
      and carrier is not null
),
joined as (
    -- Attach orders attributes
    select
        src.*,
        p.amount,
        p.status as order_status
    from base src
    left join orders p
        on src.order_id = p.id
),
metrics as (
    -- Ranking and gap features
    select
        order_id,
        shipped_date,
        carrier_filled,
        days_since_shipped_date_2,
        is_lost,
        amount,
        round(amount / sum(amount) over (partition by carrier), 4) as amount_share,
        dense_rank() over (order by is_lost desc nulls first) as is_lost_dense_rank,
        cume_dist() over (partition by order_id order by days_since_shipped_date) as days_since_shipped_date_cume_dist,
        count(is_lost) over (partition by month(shipped_date), order_id) as shipped_date_month_count_is_lost,
        percent_rank() over (partition by date_trunc('month', shipped_date), order_id order by is_lost desc) as is_lost_percent_rank
    from joined
)
-- Multi-level aggregation
select
    order_id,
    percentile_approx(amount_share, 0.9) as amount_share_p90,
    count(*) as num_rows,
    percentile_approx(is_lost_percent_rank, array(0.25, 0.5, 0.75)) as is_lost_percent_rank_quartiles
from metrics
group by 1
order by num_rows desc
