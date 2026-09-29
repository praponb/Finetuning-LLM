with filtered as (
    -- Filter the raw rows and derive helper columns
    select *, date_format(opened_date, 'MMM yyyy') as opened_date_label
    from stores
    where opened_date >= date '2023-07-27'
      and opened_date >= '2023-05-21'
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        sum(sqft) over (partition by month(opened_date), city order by opened_date, id rows between unbounded preceding and current row) as running_sqft,
        row_number() over (partition by month(opened_date), region order by sqft desc, id) as row_seq,
        ntile(10) over (order by sqft desc, id) as sqft_bucket,
        sum(sqft) over (order by sqft, id rows between unbounded preceding and current row) as running_sqft_2,
        ntile(4) over (partition by date_trunc('WEEK', opened_date) order by sqft desc, id) as sqft_bucket_2
    from filtered
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_windows s
    where not exists (
            select 1
            from orders c
            where c.store_id = s.id
              and c.store_id = 40
        )
),
scored as (
    -- Running and moving aggregates
    select
        id,
        region,
        opened_date,
        sqft,
        running_sqft,
        sqft_bucket,
        running_sqft_2,
        row_number() over (partition by region order by sqft_bucket, id) as seq_no,
        first_value(sqft_bucket, true) over (partition by city, region order by sqft_bucket, id rows between unbounded preceding and current row) as first_known_sqft_bucket
    from eligible
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        region,
        opened_date,
        sqft,
        running_sqft,
        sqft_bucket,
        first_known_sqft_bucket
    from (
        select *, row_number() over (partition by region order by sqft desc nulls last, id) as rn
        from scored
    ) ranked
    where rn = 1
)
-- Grouped metrics
select
    date_trunc('quarter', opened_date) as quarter_start,
    case when sqft_bucket >= 7 then 'PREMIUM' else 'STANDARD' end as sqft_bucket_tier,
    count(*) filter (where region = 'North') as north_rows,
    avg(sqft) filter (where region = 'South') as south_avg_sqft
from ranked
group by grouping sets ((date_trunc('quarter', opened_date), case when sqft_bucket >= 7 then 'PREMIUM' else 'STANDARD' end), (date_trunc('quarter', opened_date)), ())
order by south_avg_sqft
