with base as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        case when status in ('DELIVERED', 'LOST') then 'Tier 1' else 'Other' end as status_group,
        coalesce(carrier, 'DIRECT') as carrier_clean
    from shipments
    where delivered_date < '2024-07-13'
),
joined as (
    -- Attach orders attributes
    select
        s.*,
        b.amount,
        b.order_date,
        b.shipping_fee
    from base s
    left join orders b
        on s.order_id <=> b.id
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        order_id,
        shipped_date,
        delivered_date,
        cost,
        status_group,
        amount,
        order_date,
        row_number() over (partition by carrier order by order_date, id) as seq_no,
        row_number() over (order by delivered_date, id) as row_seq,
        sum(shipping_fee) over (partition by order_id order by cost, id rows between unbounded preceding and current row) as running_shipping_fee,
        row_number() over (partition by order_id order by order_date desc, id) as row_seq_2
    from joined
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        shipped_date,
        delivered_date,
        cost,
        status_group,
        amount,
        order_date,
        seq_no,
        row_seq,
        running_shipping_fee,
        row_seq_2,
        case when row_seq_2 >= 7 then 'High' when row_seq_2 >= 4 then 'Medium' else 'Low' end as row_seq_2_band
    from enriched
    where shipped_date > date '2023-08-04'
)
-- Multi-level aggregation
select
    date_trunc('week', shipped_date) as week_start,
    date_trunc('year', shipped_date) as year_start,
    percentile_approx(amount, array(0.25, 0.5, 0.75)) as amount_quartiles,
    percentile_approx(row_seq_2, array(0.25, 0.5, 0.75)) as row_seq_2_quartiles,
    percentile_approx(seq_no, 0.95) as seq_no_p95
from valid
group by date_trunc('week', shipped_date), date_trunc('year', shipped_date)
order by seq_no_p95 desc;
