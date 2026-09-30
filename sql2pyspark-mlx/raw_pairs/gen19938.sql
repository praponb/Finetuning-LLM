with kept_keys as (
    -- Set difference on keys
    select region
    from stores
    except
    select region
    from stores
    where month(opened_date) in (5, 7, 10)
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from stores s
    inner join kept_keys k on s.region = k.region
)
-- Grouped metrics
select
    city,
    stddev_samp(sqft) as sqft_stddev,
    count(case when city = 'New York' then 1 end) as new_york_count,
    percentile_approx(sqft, array(0.25, 0.5, 0.75)) as sqft_quartiles,
    sum(case when city = 'Bangkok' then sqft else 0 end) as bangkok_sqft
from kept_rows
group by city
having sqft_stddev > 0
order by bangkok_sqft desc;
