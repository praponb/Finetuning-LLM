with cleaned as (
    select
        *,
        lower(city) as city_lower,
        round(rating * 100, 2) as rating_adjusted,
        coalesce(city, 'DIRECT') as city_filled
    from suppliers
    where supplier_name like '%Impo%'
),
metrics as (
    select
        *,
        lag(rating_adjusted) over (partition by city, country order by rating_adjusted asc, id asc) as prev_rating_adjusted,
        lag(rating) over (partition by city, country order by rating_adjusted asc, id asc) as prev_rating,
        row_number() over (partition by city, country order by rating_adjusted asc, id asc) as seq_no,
        first_value(city) ignore nulls over (partition by country order by rating_adjusted asc, id asc rows between unbounded preceding and current row) as first_known_city
    from cleaned
    where rating is not null
),
deduped as (
    select
        id,
        supplier_name,
        city,
        rating_adjusted,
        prev_rating_adjusted,
        seq_no
    from (
        select *, rank() over (partition by city order by prev_rating desc) as row_num
        from metrics
    ) ranked
    where row_num = 1
),
base as (
    select
        id,
        supplier_name,
        rating_adjusted,
        cast(prev_rating_adjusted as int) as prev_rating_adjusted_int
    from deduped
    where city is distinct from 'London'
      and seq_no != 6
)
select
    case when rating_adjusted >= 60 then 'PREMIUM' else 'STANDARD' end as rating_adjusted_tier,
    sum(rating_adjusted) as total_rating_adjusted,
    max(prev_rating_adjusted_int) as max_prev_rating_adjusted_int,
    count(*) as row_count
from base
group by case when rating_adjusted >= 60 then 'PREMIUM' else 'STANDARD' end
order by row_count asc nulls last;
