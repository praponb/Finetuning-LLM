with deduped as (
    -- Keep one row per key (latest first)
    select *
    from suppliers
    qualify row_number() over (partition by city, country order by rating desc nulls last, id) = 1
)
select
    id,
    supplier_name,
    city,
    rating
from deduped;
