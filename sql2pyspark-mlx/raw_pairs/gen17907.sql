with filtered as (
    -- Basic cleansing and defaults
    select *, upper(trim(supplier_name)) as supplier_name_normalized
    from suppliers
    where city is distinct from 'Berlin'
      and supplier_name not like 'T%'
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        supplier_name,
        city,
        rating,
        supplier_name_normalized,
        lag(rating) over (partition by country order by rating, id) as prev_rating,
        lead(rating) over (partition by country order by rating, id) as next_rating,
        row_number() over (partition by country order by rating, id) as seq_no
    from filtered
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from enriched s
    where not exists (
            select 1
            from products c
            where c.supplier_id = s.id
              and month(c.launch_date) in (3, 9, 10)
        )
),
latest_match as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select s.*, top1.cost
    from not_flagged s
    join lateral (
            select c.cost
            from products c
            where c.supplier_id = s.id
            order by c.price desc, c.id desc
            limit 1
        ) top1 on true
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        city,
        rating,
        supplier_name_normalized,
        prev_rating,
        next_rating,
        cost,
        round(rating / seq_no, 2) as rating_per_seq_no,
        coalesce(city, 'OTHER') as city_clean
    from latest_match
    where supplier_name like '%Toky%'
),
with_last as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.city,
        s.rating,
        s.next_rating,
        s.cost,
        s.rating_per_seq_no,
        top1.product_cost,
        top1.product_launch_date
    from base s
    join lateral (
            select c.cost as product_cost, c.launch_date as product_launch_date
            from products c
            where c.supplier_id = s.id
            order by c.price desc, c.id desc
            limit 1
        ) top1 on true
)
-- Aggregate with conditional metrics
select
    date_trunc('week', product_launch_date) as week_start,
    city,
    count(distinct city) as distinct_cities,
    sum(next_rating) as total_next_rating
from with_last
group by date_trunc('week', product_launch_date), city
having distinct_cities >= 1
order by city desc
