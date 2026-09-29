with scored as (
    -- Window metrics over several different partitions
    select
        *,
        max(rating) over (partition by city) city_max_rating,
        row_number() over (partition by city order by rating desc, id asc) row_seq,
        avg(rating) over (order by rating asc, id asc rows between 6 preceding and 1 preceding) rating_avg_6_prior,
        sum(rating) over w_hist running_rating
    from suppliers
    window w_hist as (partition by city order by rating asc, id asc rows between unbounded preceding and current row)
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from scored s
    where not exists (
            select 1
            from products c
            where c.supplier_id = s.id
              and c.brand = 'Acme'
        )
),
latest as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by country order by rating_avg_6_prior desc, id asc) as row_num
    from kept
    qualify row_num = 1
)
select
    id,
    supplier_name,
    country,
    city,
    rating,
    city_max_rating,
    rating_avg_6_prior,
    running_rating,
    row_num
from latest
order by city asc;
