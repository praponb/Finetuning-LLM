with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        round(quantity * 100, 2) as quantity_scaled
    from inventory
    where store_id = 4
),
joined as (
    -- Attach stores attributes
    select
        src.*,
        b.opened_date,
        b.city
    from filtered src
    left join stores b
        on src.store_id = b.id
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        avg(quantity) over (partition by city order by quantity_scaled, id rows between 3 preceding and 1 preceding) as quantity_avg_3_prior,
        row_number() over (partition by city order by quantity_scaled, id) as seq_no,
        row_number() over (partition by city order by quantity, id) as event_seq
    from joined
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from scored
    qualify row_number() over (partition by product_id order by quantity_scaled nulls first, id) = 1
)
-- Native SQL PIVOT
select *
from (
    select
        store_id,
        city,
        quantity
    from latest
) src
pivot (
    avg(quantity) as total,
    count(1) as n
    for city in ('Chiang Mai' as chiang_mai, 'London' as london)
);
