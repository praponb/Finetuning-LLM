with FilteredShipments as (
    select *, round(cost, 1) cost_rounded
    from shipments
    where order_id is not null or order_id is null
),
shipments_stats as (
    select carrier, sort_array(collect_set(status)) status_list
    from FilteredShipments
    group by carrier
)
select *
from shipments_stats
order by carrier;
