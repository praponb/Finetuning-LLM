with base as (
    -- Keep recent, valid rows
    select
        *,
        initcap(trim(city)) as city_title,
        initcap(trim(segment)) as segment_title
    from customers
    where city is not null
      and email like '%@startup.io'
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from base s
    where not exists (
            select 1
            from orders c
            where c.customer_id = s.id
              and c.amount <= 550
        )
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        name,
        email,
        city,
        is_active,
        age,
        segment_title
    from (
        select *, rank() over (partition by country order by signup_date nulls first) as rank_in_group
        from kept
    ) x
    where rank_in_group = 1
),
kept_keys as (
    -- Set difference on keys
    select id, city
    from deduped
    except
    select id, city
    from customers
    where email like '%@company.co'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from deduped s
    inner join kept_keys k on s.id = k.id and s.city = k.city
),
scored as (
    -- Ranking and gap features
    select
        id,
        name,
        city,
        age,
        segment_title,
        cume_dist() over (partition by city order by age nulls first) as age_cume_dist,
        round(age / sum(age) over (partition by city), 4) as age_share,
        rank() over (order by age desc nulls last) as age_rank,
        rank() over (partition by city order by age desc nulls first) as age_rank_2,
        stddev_samp(age) over (partition by city) as city_stddev_samp_age
    from verified
),
enriched as (
    -- Running and moving aggregates
    select
        segment_title,
        age_cume_dist,
        age_rank_2,
        city_stddev_samp_age,
        dense_rank() over (partition by city order by age_share desc nulls last) as age_share_dense_rank,
        cume_dist() over (partition by city order by age_cume_dist desc nulls last) as age_cume_dist_cume_dist,
        dense_rank() over (order by city_stddev_samp_age desc nulls last) as city_stddev_samp_age_dense_rank
    from scored
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        city_stddev_samp_age_dense_rank * age_cume_dist as city_stddev_samp_age_dense_rank_times_age_cume_dist,
        city_stddev_samp_age_dense_rank + age_cume_dist_cume_dist as city_stddev_samp_age_dense_rank_plus_age_cume_dist_cume_dist
    from enriched
    where age_cume_dist < 0.25
),
summary as (
    -- Grouped metrics
    select
        case when city_stddev_samp_age_dense_rank >= 8 then 'HIGH' else 'LOW' end as city_stddev_samp_age_dense_rank_tier,
        approx_percentile(city_stddev_samp_age_dense_rank_plus_age_cume_dist_cume_dist, 0.95) as city_stddev_samp_age_dense_rank_plus_age_cume_dist_cume_dist_p95,
        percentile_approx(age_share_dense_rank, array(0.25, 0.5, 0.75)) as age_share_dense_rank_quartiles,
        approx_percentile(age_cume_dist_cume_dist, 0.9) as age_cume_dist_cume_dist_p90,
        approx_percentile(age_cume_dist_cume_dist, 0.9) as age_cume_dist_cume_dist_p90_2
    from valid
    group by city_stddev_samp_age_dense_rank_tier
)
select
    city_stddev_samp_age_dense_rank_tier,
    age_share_dense_rank_quartiles,
    age_cume_dist_cume_dist_p90,
    age_cume_dist_cume_dist_p90_2
from summary
order by age_cume_dist_cume_dist_p90 desc nulls first;
