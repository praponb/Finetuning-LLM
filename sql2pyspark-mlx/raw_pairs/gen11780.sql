with filtered as (
    -- Basic cleansing and defaults
    select
        *,
        round(rating * 0.85, 2) as rating_scaled,
        case when country = 'SG' then 1 else 0 end as is_sg,
        coalesce(city, 'DIRECT') as city_clean
    from suppliers
    where country = 'TH'
),
latest as (
    -- QUALIFY-style window filter
    select
        id,
        rating_scaled,
        is_sg,
        city_clean
    from filtered
    qualify rank() over (partition by city, country order by rating_scaled nulls last) = 1
),
remaining_keys as (
    -- Set difference on keys
    select id
    from latest
    except all
    select id
    from suppliers
    where supplier_name like '%Who%'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from latest s
    inner join remaining_keys k on s.id = k.id
),
cleaned as (
    -- Keep recent, valid rows
    select *, round(is_sg, 0) as is_sg_rounded
    from verified
    where rating_scaled < 37
),
scored as (
    -- Ranking and gap features
    select
        id,
        rating_scaled,
        percent_rank() over (order by rating_scaled desc nulls first) as rating_scaled_percent_rank,
        cume_dist() over w_part as is_sg_cume_dist,
        percent_rank() over (order by is_sg_rounded desc) as is_sg_rounded_percent_rank
    from cleaned
    window w_part as (order by is_sg nulls first)
)
-- Multi-level aggregation
select
    case when rating_scaled_percent_rank >= 0.85 then 'HIGH' else 'LOW' end as rating_scaled_percent_rank_tier,
    case when is_sg_rounded_percent_rank >= 0.78 then 'LARGE' else 'SMALL' end as is_sg_rounded_percent_rank_tier,
    count(*) as row_count,
    max(is_sg_cume_dist) as max_is_sg_cume_dist,
    count(distinct id) as distinct_ids,
    round(avg(rating_scaled_percent_rank), 2) as avg_rating_scaled_percent_rank,
    count(*) as cnt
from scored
group by case when rating_scaled_percent_rank >= 0.85 then 'HIGH' else 'LOW' end, case when is_sg_rounded_percent_rank >= 0.78 then 'LARGE' else 'SMALL' end
order by max_is_sg_cume_dist nulls first;
