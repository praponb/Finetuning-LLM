with cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, case when rating >= 3.4 then 'High' when rating >= 1.8 then 'Medium' else 'Low' end as rating_tier
    from suppliers
    where city is not null
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        country,
        rating,
        round(rating / sum(rating) over (partition by country, city), 4) as rating_share,
        min(rating) over (partition by country) as country_min_rating,
        avg(rating) over (partition by city, country order by rating, id rows between 3 preceding and current row) as rating_avg_3
    from cleaned
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        country,
        rating,
        rating_share,
        country_min_rating,
        rating_avg_3,
        initcap(trim(country)) as country_title
    from metrics
    where rating <= 2.4
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from base s
    where exists (
            select 1
            from products x
            where x.supplier_id = s.id
              and x.cost <= 580
        )
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        country,
        rating_share,
        country_min_rating
    from kept
    qualify row_number() over (partition by country order by country_min_rating desc nulls first, id) = 1
)
-- Multi-level aggregation
select
    country,
    case when rating_share >= 0.88 then 'LARGE' else 'SMALL' end as rating_share_tier,
    count(*) as event_count,
    count_if(country = 'TH') as th_events,
    sort_array(collect_set(country)) as country_list,
    percent_rank() over (partition by country order by count_if(country = 'TH') desc) as th_events_percent_rank
from latest
group by country, case when rating_share >= 0.88 then 'LARGE' else 'SMALL' end
having count(*) > 1
