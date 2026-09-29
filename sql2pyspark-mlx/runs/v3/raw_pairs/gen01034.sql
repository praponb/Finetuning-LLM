with base as (
    select *
    from shipments
    where carrier in ('FedEx', 'Kerry', 'DHL') and shipped_date between '2025-06-03' and '2025-08-01'
),
delivered_year_summary as (
    select year(delivered_date) delivered_year, status, concat_ws(', ', sort_array(collect_set(carrier))) carrier_values
    from base
    group by year(delivered_date), status
)
select *
from delivered_year_summary;
