with metrics_long as (
    -- Native SQL UNPIVOT (wide to long)
    select
        id,
        status,
        measure,
        value
    from (
        select
            id,
            status,
            discount,
            shipping_fee,
            amount
        from orders
    ) src
    unpivot (
        value for measure in (discount, shipping_fee, amount)
    ) long_rows
)
-- Multi-level aggregation
select
    measure,
    case when value >= 63 then 'HIGH' else 'LOW' end as value_tier,
    max(value) filter (where measure = 'shipping_fee') as shipping_fee_max_value,
    max(value) filter (where measure = 'discount') as discount_max_value,
    grouping(measure) as grouping_id
from metrics_long
group by cube (measure, case when value >= 63 then 'HIGH' else 'LOW' end)
having shipping_fee_max_value >= 0
order by shipping_fee_max_value desc nulls first
