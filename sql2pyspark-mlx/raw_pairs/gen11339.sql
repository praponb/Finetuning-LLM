with cleaned as (
    -- Basic cleansing and defaults
    select *, date_format(shipped_date, 'yyyy-MM-dd') as shipped_date_label
    from shipments
    where carrier = 'Flash'
      and shipped_date >= date '2023-09-18'
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        order_id,
        shipped_date,
        delivered_date,
        cost,
        status,
        shipped_date_label,
        lower(status) as status_lower,
        datediff(current_date(), shipped_date) as days_since_shipped_date
    from cleaned
    where delivered_date >= date '2023-04-02'
),
enriched as (
    -- Attach orders attributes
    select
        src.*,
        o.discount,
        o.channel,
        o.order_date
    from valid src
    left join orders o
        on src.order_id = o.id
    where o.store_id = 10
),
joined as (
    -- Attach orders attributes
    select
        src.id,
        src.order_id,
        src.delivered_date,
        src.days_since_shipped_date,
        src.discount,
        b.discount as order_discount,
        b.order_date as order_order_date
    from enriched src
    left join orders b
        on src.order_id = b.id
),
grouped as (
    -- Multi-level aggregation
    select
        date_trunc('quarter', order_order_date) as quarter_start,
        case when days_since_shipped_date >= 66 then 'PREMIUM' else 'STANDARD' end as days_since_shipped_date_tier,
        percentile_approx(days_since_shipped_date, 0.95) as days_since_shipped_date_p95,
        count(*) as row_count,
        percentile_approx(discount, 0.95) as discount_p95,
        percentile_approx(discount, array(0.25, 0.5, 0.75)) as discount_quartiles,
        approx_percentile(order_discount, 0.9) as order_discount_p90
    from joined
    group by quarter_start, days_since_shipped_date_tier
)
select
    quarter_start,
    days_since_shipped_date_tier,
    days_since_shipped_date_p95,
    row_count,
    discount_p95,
    order_discount_p90
from grouped;
