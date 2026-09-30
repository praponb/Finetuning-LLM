with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        carrier,
        shipped_date,
        cost,
        status,
        length(status) as status_length,
        coalesce(cost, 0) as cost_filled
    from shipments
    where shipped_date > date '2023-09-18'
      and carrier is not null
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        carrier,
        cost,
        cost_filled
    from filtered
) src
unpivot (
    val for metric_name in (cost as COST, cost_filled as COST_FILLED)
);
