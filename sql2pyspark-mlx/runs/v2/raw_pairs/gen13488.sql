with FilteredShipments as (
    select *
    from shipments
    where carrier = 'UPS'
),
agg as (
    select
        status,
        quarter(shipped_date) as shipped_quarter,
        sum(case when carrier = 'Kerry' then cost else 0 end) as kerry_cost,
        max(shipped_date) as latest_shipped_date
    from FilteredShipments
    group by status, quarter(shipped_date)
)
select
    status,
    shipped_quarter,
    kerry_cost,
    latest_shipped_date
from agg
where kerry_cost < 26
