with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        product_name,
        brand,
        price,
        cost,
        launch_date,
        array_join(tags, ', ') as tags_text,
        coalesce(brand, 'OTHER') as brand_clean
    from products
    where product_name is not null
      and launch_date > date '2023-07-24'
),
metrics as (
    -- Ranking and gap features
    select
        id,
        brand,
        tags_text,
        brand_clean,
        lag(cost) over (partition by brand order by price, id) as prev_cost,
        lag(price) over (partition by brand order by price, id) as prev_price,
        lead(price) over (partition by brand order by price, id) as next_price,
        row_number() over (partition by brand order by price, id) as seq_no,
        dense_rank() over (partition by brand order by price nulls last) as price_dense_rank
    from base
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from metrics s
    where exists (
            select 1
            from store_sales x
            where x.product_id = s.id
              and x.units != 23
        )
),
ranked as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by brand order by price_dense_rank desc, id) as rank_in_group
    from eligible
    qualify rank_in_group = 1
),
with_last as (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.quantity,
        latest.last_restock
    from ranked s
    left join lateral (
            select c.quantity, c.last_restock
            from inventory c
            where c.product_id = s.id
            order by c.last_restock desc, c.id desc
            limit 1
        ) latest on true
),
deduped as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by brand order by prev_cost nulls last, id) as rn
    from with_last
    qualify rn = 1
),
latest as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by brand order by seq_no desc, id) as row_num
    from deduped
    qualify row_num = 1
)
-- Grouped metrics
select
    seq_no,
    percentile_approx(prev_cost, 0.95) as prev_cost_p95,
    approx_percentile(seq_no, array(0.25, 0.5, 0.75)) as seq_no_quartiles,
    round(avg(prev_price), 2) as avg_prev_price
from latest
group by seq_no;
