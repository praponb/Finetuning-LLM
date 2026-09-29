with latest_match as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.category,
        latest.cost
    from suppliers s
    join lateral (
            select c.category, c.cost
            from products c
            where c.supplier_id = s.id
            order by c.cost desc, c.id desc
            limit 1
        ) latest on true
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from latest_match s
    where not exists (
            select 1
            from products x
            where x.supplier_id = s.id
              and size(x.tags) >= 1
        )
),
matched_keys as (
    -- Set difference on keys
    select city, id
    from eligible
    except all
    select city, id
    from suppliers
    where country in ('UK', 'US', 'DE')
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from eligible s
    inner join matched_keys k on s.city = k.city and s.id = k.id
),
scored as (
    -- Running and moving aggregates
    select
        *,
        cume_dist() over w_hist as rating_cume_dist,
        count(cost) over (partition by city, country) as city_count_cost,
        dense_rank() over w_part as rating_dense_rank
    from reconciled
    window w_hist as (order by rating desc), w_part as (partition by category, city order by rating asc nulls last)
),
scored_suppliers as (
    -- Window metrics over several different partitions
    select
        id,
        supplier_name,
        country,
        city,
        rating,
        category,
        cost,
        rating_cume_dist,
        city_count_cost,
        rating_dense_rank,
        round(rating_dense_rank / sum(rating_dense_rank) over (partition by category), 4) as rating_dense_rank_share,
        round(city_count_cost / sum(city_count_cost) over (partition by city), 4) as city_count_cost_share,
        rank() over (partition by country order by city_count_cost asc) as city_count_cost_rank
    from scored
),
stats as (
    -- Aggregate with conditional metrics
    select
        city,
        approx_percentile(city_count_cost_share, array(0.25, 0.5, 0.75)) as city_count_cost_share_quartiles,
        round(avg(rating_dense_rank_share), 2) as avg_rating_dense_rank_share,
        sum(case when city = 'Singapore' then rating_dense_rank_share else 0 end) as singapore_rating_dense_rank_share,
        round(avg(city_count_cost_rank), 2) as avg_city_count_cost_rank,
        size(collect_set(city)) as num_cities,
        count_if(country = 'UK') as uk_events
    from scored_suppliers
    group by 1
)
select
    city,
    avg_rating_dense_rank_share,
    avg_city_count_cost_rank,
    num_cities,
    uk_events
from stats;
