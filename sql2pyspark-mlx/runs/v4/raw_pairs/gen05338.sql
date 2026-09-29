with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        last_day(shipped_date) as shipped_date_month_end,
        datediff(shipped_date, delivered_date) as days_delivered_date_to_shipped_date,
        coalesce(carrier, 'NONE') as carrier_filled
    from shipments
    where delivered_date >= date '2023-05-19'
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by order_id order by shipped_date_month_end, id) as rn
        from base
    ) t
    where rn = 1
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        carrier,
        delivered_date,
        status,
        shipped_date_month_end,
        rn,
        last_day(shipped_date) as shipped_date_month_end_2,
        add_months(delivered_date, 3) as delivered_date_plus_months,
        coalesce(carrier, 'DIRECT') as carrier_clean
    from latest
    where shipped_date >= date '2024-01-26'
      and order_id is not null
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        carrier,
        delivered_date,
        status,
        shipped_date_month_end,
        rn,
        shipped_date_month_end_2,
        delivered_date_plus_months,
        carrier_clean,
        last_value(carrier, true) over (partition by carrier order by shipped_date_month_end_2, id rows between unbounded preceding and current row) as last_known_carrier,
        last_value(carrier, true) over (partition by carrier order by shipped_date_month_end, id rows between unbounded preceding and current row) as last_known_carrier_2,
        first_value(carrier) ignore nulls over (partition by status order by shipped_date_month_end, id rows between unbounded preceding and current row) as first_known_carrier
    from cleaned
)
select
    id,
    rn,
    shipped_date_month_end_2,
    delivered_date_plus_months,
    carrier_clean,
    last_known_carrier,
    last_known_carrier_2,
    first_known_carrier
from with_windows;
