with base as (
    select
        id,
        store_name,
        city,
        region,
        opened_date,
        length(city) as city_length,
        coalesce(region, 'NONE') as region_filled
    from stores
    where region is not null
      and length(store_name) > 6
),
metrics as (
    select
        id,
        city_length,
        region_filled,
        first_value(city_length) ignore nulls over (order by city_length, id rows between unbounded preceding and current row) as first_known_city_length,
        lead(city_length, 1) over (partition by month(opened_date) order by opened_date, id) as next_city_length,
        avg(city_length) over (order by opened_date, id rows between 3 preceding and current row) as city_length_avg_3,
        row_number() over w_hist as row_seq,
        lag(city_length, 2) over w_part as prev_city_length
    from base
    window w_hist as (partition by date_trunc('YEAR', opened_date), city order by opened_date desc, id), w_part as (partition by region order by city_length, id)
),
kept as (
    select s.*
    from metrics s
    where not exists (
            select 1
            from orders x
            where x.store_id = s.id
              and x.channel in ('web', 'mobile')
        )
),
base_stores as (
    select *, first_known_city_length + city_length_avg_3 as first_known_city_length_plus_city_length_avg_3
    from kept
    where first_known_city_length >= 33
),
metrics_stores as (
    select
        *,
        percent_rank() over (order by prev_city_length nulls last) as prev_city_length_percent_rank,
        row_number() over (order by row_seq desc, id) as event_seq,
        sum(city_length) over (order by row_seq, id rows between unbounded preceding and current row) as cumulative_city_length,
        dense_rank() over (order by city_length_avg_3) as city_length_avg_3_dense_rank
    from base_stores
)
select
    id,
    region_filled,
    first_known_city_length,
    next_city_length,
    city_length_avg_3,
    row_seq,
    prev_city_length,
    first_known_city_length_plus_city_length_avg_3,
    prev_city_length_percent_rank
from metrics_stores
order by row_seq desc
