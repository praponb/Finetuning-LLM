with valid as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        coalesce(carrier, 'unassigned') carrier_filled,
        date_trunc('YEAR', delivered_date) delivered_date_period
    from shipments
    where shipped_date >= date '2023-03-28'
),
base as (
    -- Keep recent, valid rows
    select
        id,
        order_id,
        carrier,
        carrier_filled,
        initcap(trim(carrier)) carrier_title
    from valid
    where delivered_date >= date '2023-04-09'
      and order_id is not null
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        case when carrier in ('Kerry', 'UPS') then 'Tier 1' else 'Other' end carrier_group,
        initcap(trim(carrier)) carrier_title_2,
        coalesce(carrier, 'UNKNOWN') carrier_clean
    from base
    where order_id = 19
)
-- Grouped metrics
select
    carrier,
    count(distinct id) as distinct_ids,
    count(*) filter (where carrier = 'Kerry') as kerry_rows,
    count(case when carrier = 'FedEx' then 1 end) as fedex_count,
    count(case when carrier = 'FedEx' then 1 end) as fedex_count_2
from filtered
group by carrier
order by fedex_count_2 desc nulls last;
