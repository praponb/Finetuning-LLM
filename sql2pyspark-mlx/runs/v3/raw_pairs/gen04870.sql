with base as (
    select *
    from shipments
    where status = 'DELIVERED'
),
summary as (
    select carrier, max(delivered_date) as latest_delivered_date, sort_array(collect_set(carrier)) as carrier_list
    from base
    group by carrier
)
select carrier, carrier_list
from summary
order by carrier
