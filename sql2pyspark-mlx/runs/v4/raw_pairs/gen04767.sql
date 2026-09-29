with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        status,
        date_trunc('year', shipped_date) as shipped_date_period,
        coalesce(carrier, 'OTHER') as carrier_filled
    from shipments
    where shipped_date >= date '2024-01-04'
),
matched_keys as (
    -- Set intersection on keys
    select carrier
    from base
    intersect
    select carrier
    from shipments
    where cost < 79
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from base s
    inner join matched_keys k on s.carrier = k.carrier
),
with_dims as (
    -- Attach orders attributes
    select
        s.*,
        b.status as order_status,
        b.amount
    from kept_rows s
    inner join orders b
        on s.order_id = b.id
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        max(cost) over (partition by order_id order by cast(shipped_date_period as bigint) asc range between 7776000 preceding and current row) as cost_max_90d,
        stddev_samp(amount) over (partition by carrier) as carrier_stddev_samp_amount,
        round(cost / sum(cost) over (partition by carrier), 4) as cost_share,
        max(cost) over (partition by order_id order by cast(shipped_date_period as bigint) asc range between 604800 preceding and current row) as cost_max_7d
    from with_dims
)
-- Ranking and gap features
select
    *,
    rank() over (partition by order_status order by cost_share desc) as cost_share_rank,
    rank() over (partition by order_id order by cost_max_90d asc) as cost_max_90d_rank,
    percent_rank() over (partition by carrier order by cost desc) as cost_percent_rank
from metrics
