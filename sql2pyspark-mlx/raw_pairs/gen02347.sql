with valid as (
    select *, date_trunc('WEEK', last_restock) as last_restock_period
    from inventory
    where last_restock > date '2023-05-21'
),
joined as (
    select
        e.*,
        p.product_name,
        p.price
    from valid e
    left join products p
        on e.product_id = p.id
    where p.price is null
),
joined_inventory as (
    select
        s.id,
        s.product_id,
        s.product_name,
        s.price,
        p.price as product_price,
        p.category
    from joined s
    left join products p
        on s.product_id = p.id
),
ranked as (
    select *
    from (
        select *, row_number() over (partition by product_id order by product_price desc nulls first, id) as latest_rank
        from joined_inventory
    ) ranked
    where latest_rank = 1
),
deduped as (
    select *
    from (
        select *, row_number() over (partition by category, product_id order by product_price desc, id) as row_num
        from ranked
    ) x
    where row_num = 1
)
select
    category,
    percentile_approx(product_price, array(0.25, 0.5, 0.75)) as product_price_quartiles,
    count_if(category = 'Home') as home_events,
    percentile_approx(price, array(0.25, 0.5, 0.75)) as price_quartiles,
    count(*) as cnt,
    count(distinct id) as distinct_ids
from deduped
group by cube (category)
having cnt < 1000;
