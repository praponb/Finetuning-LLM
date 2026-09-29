with filtered as (
    select
        id,
        store_name,
        city,
        region,
        opened_date,
        datediff(current_date(), opened_date) / 365 as years_since_opened_date,
        initcap(trim(region)) as region_title
    from stores
    where region is not null
      and region = 'North'
),
ranked as (
    select
        id,
        city,
        years_since_opened_date,
        region_title
    from filtered
    qualify row_number() over (partition by region order by years_since_opened_date desc, id) = 1
),
with_last as (
    select s.*, top1.order_date
    from ranked s
    left join lateral (
            select c.order_date
            from orders c
            where c.store_id = s.id
            order by c.order_date desc, c.id desc
            limit 1
        ) top1 on true
),
enriched as (
    select
        id,
        city,
        years_since_opened_date,
        region_title,
        max(years_since_opened_date) over w_recent as running_max_years_since_opened_date,
        sum(years_since_opened_date) over w_recent as running_sum_years_since_opened_date,
        min(years_since_opened_date) over w_recent as running_min_years_since_opened_date,
        row_number() over w_part as seq_no
    from with_last
    window w_recent as (partition by city order by years_since_opened_date, id rows between unbounded preceding and current row), w_part as (partition by city order by years_since_opened_date desc, id)
),
latest_match as (
    select s.*, top1.order_date
    from enriched s
    join lateral (
            select c.order_date
            from orders c
            where c.store_id = s.id
            order by c.shipping_fee desc, c.id desc
            limit 1
        ) top1 on true
)
select
    case when running_sum_years_since_opened_date >= 350 then 'PREMIUM' else 'STANDARD' end as running_sum_years_since_opened_date_tier,
    city,
    percentile_approx(running_max_years_since_opened_date, 0.5) as running_max_years_since_opened_date_p50,
    count(case when city = 'Chiang Mai' then 1 end) as chiang_mai_count,
    count(distinct city) as distinct_cities,
    count(distinct city) as distinct_cities_2,
    count(distinct city) as distinct_cities_3,
    sum(case when city = 'London' then seq_no else 0 end) as london_seq_no
from latest_match
group by cube (case when running_sum_years_since_opened_date >= 350 then 'PREMIUM' else 'STANDARD' end, city)
order by distinct_cities nulls last;
