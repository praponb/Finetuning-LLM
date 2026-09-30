with not_flagged as (
    select s.*
    from stores s
    where exists (
            select 1
            from inventory x
            where x.store_id = s.id
              and x.last_restock < s.opened_date
        )
),
deduped as (
    select *
    from (
        select *, row_number() over (partition by city order by opened_date desc nulls last, id) as row_num
        from not_flagged
    ) ranked
    where row_num = 1
),
latest_match as (
    select
        s.id,
        s.region,
        s.sqft,
        top1.quantity
    from deduped s
    join lateral (
            select c.quantity
            from inventory c
            where c.store_id = s.id
              and c.last_restock <= s.opened_date
            order by c.last_restock desc, c.id desc
            limit 1
        ) top1 on true
),
base as (
    select
        *,
        sqft / nullif(quantity, 0) sqft_per_quantity,
        lower(region) region_lower
    from latest_match
    where region is not null
),
enriched as (
    select
        id,
        sqft,
        quantity,
        avg(quantity) over w_recent last3_avg_quantity,
        min(quantity) over w_recent last3_min_quantity,
        sum(quantity) over w_recent last3_sum_quantity,
        count(quantity) over w_recent last3_count_quantity
    from base
    where quantity != 150
    window w_recent as (partition by region order by sqft_per_quantity, id rows between 2 preceding and current row)
),
valid as (
    select
        *,
        sqft + quantity sqft_plus_quantity,
        round(quantity, 2) quantity_rounded
    from enriched
    where sqft < 6383
)
select
    id,
    sqft,
    quantity,
    last3_avg_quantity,
    last3_min_quantity,
    last3_sum_quantity,
    sqft_plus_quantity,
    quantity_rounded
from valid
order by last3_sum_quantity desc, quantity
