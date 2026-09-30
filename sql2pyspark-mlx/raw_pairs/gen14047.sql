with base as (
    select *, case when revenue >= 3300 then 'High' when revenue >= 900 then 'Medium' else 'Low' end revenue_tier
    from store_sales
    where sale_date >= date '2023-05-11'
),
metrics as (
    select
        id,
        store_id,
        sale_date,
        revenue_tier,
        lead(units) over (partition by product_id order by sale_date, id) next_units,
        lag(units) over (partition by product_id order by sale_date, id) prev_units,
        lag(revenue) over (partition by product_id order by sale_date, id) prev_revenue,
        lead(revenue) over (partition by product_id order by sale_date, id) next_revenue,
        row_number() over (partition by product_id order by sale_date, id) event_seq,
        ntile(4) over (partition by store_id order by units, id) units_bucket
    from base
),
valid as (
    select *, cast(next_revenue as int) next_revenue_int
    from metrics
    where sale_date > date '2023-04-09'
      and store_id in (4, 6)
),
ranked as (
    select
        id,
        store_id,
        sale_date,
        prev_units,
        prev_revenue,
        next_revenue,
        event_seq,
        units_bucket,
        next_revenue_int
    from (
        select *, row_number() over (partition by store_id order by sale_date desc nulls first, id) as latest_rank
        from valid
    ) ranked
    where latest_rank = 1
),
joined as (
    select
        e.*,
        s.city,
        s.opened_date
    from ranked e
    left join stores s
        on e.store_id = s.id
    where s.opened_date >= '2023-08-01'
)
select
    id,
    store_id,
    prev_units,
    prev_revenue,
    next_revenue,
    units_bucket,
    next_revenue_int,
    city,
    opened_date
from joined
order by prev_units;
