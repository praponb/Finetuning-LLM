with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        store_name,
        city,
        opened_date,
        last_day(opened_date) as opened_date_month_end,
        coalesce(region, 'OTHER') as region_clean
    from stores
    where opened_date >= date '2023-05-11'
),
with_last as (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    select s.*, top1.quantity
    from cleaned s
    join lateral (
            select c.quantity
            from inventory c
            where c.store_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) top1 on true
),
metrics as (
    -- Ranking and gap features
    select
        *,
        dense_rank() over (partition by city order by quantity desc nulls last) as quantity_dense_rank,
        ntile(3) over (partition by city order by quantity desc, id) as quantity_bucket,
        last_value(quantity) ignore nulls over w_hist as last_known_quantity,
        first_value(quantity) ignore nulls over w_grp as first_known_quantity
    from with_last
    where opened_date_month_end > '2025-02-01'
    window w_hist as (partition by city order by opened_date, id rows between unbounded preceding and current row), w_grp as (partition by month(opened_date), city order by opened_date_month_end, id rows between unbounded preceding and current row)
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from metrics s
    where not exists (
            select 1
            from inventory x
            where x.store_id = s.id
              and x.last_restock >= s.opened_date
        )
),
eligible_stores as (
    -- Anti-join via a correlated subquery
    select s.*
    from eligible s
    where not exists (
            select 1
            from store_sales c
            where c.store_id = s.id
              and c.product_id in (2, 5, 7)
        )
)
select
    id,
    city,
    opened_date,
    opened_date_month_end,
    quantity,
    quantity_dense_rank,
    quantity_bucket,
    last_known_quantity,
    first_known_quantity,
    case when city rlike '^(London|Singapore)$' then 'PRIORITY' when city rlike '^(Bangkok|Chiang Mai)$' then 'GROUP_A' else 'CORE' end as city_zone
from eligible_stores
order by city desc nulls last;
