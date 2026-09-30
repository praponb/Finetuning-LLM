with base as (
    -- Filter the raw rows and derive helper columns
    select *, coalesce(rating, 0) as rating_filled
    from suppliers
    where city is distinct from 'Singapore'
      and country in ('SG', 'DE')
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        city,
        rating_filled,
        sum(rating_filled) over (partition by city, country order by rating, id rows between 6 preceding and current row) as last7_sum_rating_filled,
        min(rating_filled) over (partition by city, country order by rating, id rows between 6 preceding and current row) as last7_min_rating_filled,
        count(rating_filled) over (partition by city, country order by rating, id rows between 6 preceding and current row) as last7_count_rating_filled,
        row_number() over (order by rating_filled, id) as event_seq
    from base
),
with_latest as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.city,
        s.last7_sum_rating_filled,
        x.price,
        x.product_stock_qty
    from with_windows s
    join lateral (
            select c.price, c.stock_qty as product_stock_qty
            from products c
            where c.supplier_id = s.id
            order by c.stock_qty desc, c.id desc
            limit 1
        ) x on true
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from with_latest s
    where exists (
            select 1
            from products x
            where x.supplier_id = s.id
              and x.supplier_id is not null
        )
),
latest_match as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.product_price,
        latest.stock_qty
    from eligible s
    left join lateral (
            select c.price as product_price, c.stock_qty
            from products c
            where c.supplier_id = s.id
            order by c.cost desc, c.id desc
            limit 1
        ) latest on true
),
latest_match_suppliers as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.product_stock_qty,
        s.stock_qty,
        x.cost
    from latest_match s
    join lateral (
            select c.cost
            from products c
            where c.supplier_id = s.id
            order by c.price desc, c.id desc
            limit 1
        ) x on true
),
scored as (
    -- Ranking and gap features
    select
        *,
        avg(cost) over w_ordered as running_avg_cost,
        sum(cost) over w_ordered as running_sum_cost,
        count(cost) over w_ordered as running_count_cost,
        last_value(product_stock_qty) ignore nulls over w_hist as last_known_product_stock_qty
    from latest_match_suppliers
    window w_ordered as (order by cost, id rows between unbounded preceding and current row), w_hist as (order by stock_qty, id rows between unbounded preceding and current row)
)
-- Multi-level aggregation
select
    case when product_stock_qty >= 370 then 'HIGH' else 'LOW' end as product_stock_qty_tier,
    case when stock_qty >= 438 then 'HIGH' else 'LOW' end as stock_qty_tier,
    count(distinct id) as distinct_ids,
    percentile_approx(running_sum_cost, array(0.25, 0.5, 0.75)) as running_sum_cost_quartiles,
    count(distinct id) as distinct_ids_2,
    percentile_approx(cost, 0.5) as cost_p50,
    min(running_sum_cost) as min_running_sum_cost,
    approx_percentile(running_count_cost, 0.5) as running_count_cost_p50
from scored
group by case when product_stock_qty >= 370 then 'HIGH' else 'LOW' end, case when stock_qty >= 438 then 'HIGH' else 'LOW' end;
