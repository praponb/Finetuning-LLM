with latest_match as (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.quantity,
        latest.unit_price
    from products s
    left join lateral (
            select c.quantity, c.unit_price
            from order_items c
            where c.product_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) latest on true
),
cleaned as (
    -- Keep recent, valid rows
    select *, dayofweek(launch_date) as launch_date_dayofweek
    from latest_match
    where launch_date > date '2023-08-07'
),
with_latest as (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.product_name,
        s.brand,
        s.price,
        s.stock_qty,
        s.launch_date,
        s.tags,
        s.supplier_id,
        s.unit_price,
        top1.store_sale_units,
        top1.store_sale_sale_date
    from cleaned s
    left join lateral (
            select c.units as store_sale_units, c.sale_date as store_sale_sale_date
            from store_sales c
            where c.product_id = s.id
            order by c.units desc, c.id desc
            limit 1
        ) top1 on true
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_latest s
    where not exists (
            select 1
            from store_sales x
            where x.product_id = s.id
              and x.sale_date <= s.launch_date
        )
)
-- Multi-level aggregation
select
    case when unit_price >= 320 then 'HIGH' else 'LOW' end as unit_price_tier,
    case when store_sale_units >= 37 then 'PREMIUM' else 'STANDARD' end as store_sale_units_tier,
    date_trunc('month', store_sale_sale_date) as activity_month,
    sum(case when brand = 'Stark' then unit_price else 0 end) as stark_unit_price,
    count(*) as row_count,
    bit_and(stock_qty) as stock_qty_bit_and,
    size(collect_set(brand)) as num_brands,
    count(case when brand = 'Acme' then 1 end) as acme_count,
    grouping_id() as grouping_id
from eligible
group by cube (case when unit_price >= 320 then 'HIGH' else 'LOW' end, case when store_sale_units >= 37 then 'PREMIUM' else 'STANDARD' end, date_trunc('month', store_sale_sale_date));
