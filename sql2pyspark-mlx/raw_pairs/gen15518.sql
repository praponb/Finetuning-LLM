with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        store_name,
        city,
        region,
        opened_date,
        concat(store_name, ' ', region) as store_name_region,
        trim(city) as clean_city,
        coalesce(region, 'DIRECT') as region_filled
    from stores
    where opened_date >= to_date('2023-08-14')
      and region is not null
),
with_last as (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.region,
        s.opened_date,
        s.store_name_region,
        s.clean_city,
        s.region_filled,
        x.last_restock,
        x.quantity
    from cleaned s
    left join lateral (
            select c.last_restock, c.quantity
            from inventory c
            where c.store_id = s.id
            order by c.last_restock desc, c.id desc
            limit 1
        ) x on true
),
latest_match as (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    select s.*, top1.store_sale_units
    from with_last s
    join lateral (
            select c.units as store_sale_units
            from store_sales c
            where c.store_id = s.id
            order by c.units desc, c.id desc
            limit 1
        ) top1 on true
),
metrics as (
    -- Ranking and gap features
    select
        id,
        opened_date,
        store_name_region,
        clean_city,
        count(quantity) over w_ordered as last3_count_quantity,
        sum(quantity) over w_ordered as last3_sum_quantity,
        max(quantity) over w_ordered as last3_max_quantity
    from latest_match
    where region = 'West'
    window w_ordered as (partition by region order by quantity, id rows between 2 preceding and current row)
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from metrics s
    where not exists (
            select 1
            from orders c
            where c.store_id = s.id
              and c.shipping_fee is not null
        )
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from eligible s
    where not exists (
            select 1
            from inventory ch
            where ch.store_id = s.id
              and ch.quantity != 76
        )
)
-- Native SQL UNPIVOT (wide to long)
select
    id,
    metric_name,
    reading
from (
    select
        id,
        last3_sum_quantity,
        last3_max_quantity
    from not_flagged
) src
unpivot (
    reading for metric_name in (last3_sum_quantity as LAST3_SUM_QUANTITY, last3_max_quantity)
)
