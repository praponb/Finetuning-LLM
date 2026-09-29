with deduped as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by carrier order by delivered_date desc nulls last, id) as rn
    from shipments
    qualify rn = 1
)
select
    id,
    order_id,
    shipped_date,
    delivered_date,
    cost,
    status
from deduped
