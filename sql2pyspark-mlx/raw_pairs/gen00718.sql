with not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from stores s
    where not exists (
            select 1
            from inventory ch
            where ch.store_id = s.id
              and ch.quantity <> 88
        )
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from not_flagged s
    where exists (
            select 1
            from store_sales ch
            where ch.store_id = s.id
              and ch.sale_date >= s.opened_date
        )
),
enriched as (
    -- Ranking and gap features
    select
        id,
        city,
        region,
        opened_date,
        sqft,
        min(sqft) over (partition by city order by opened_date, id rows between 2 preceding and current row) last3_min_sqft,
        avg(sqft) over (partition by city order by opened_date, id rows between 2 preceding and current row) last3_avg_sqft,
        count(sqft) over (partition by city order by opened_date, id rows between 2 preceding and current row) last3_count_sqft,
        sum(sqft) over (partition by city order by opened_date, id rows between 2 preceding and current row) last3_sum_sqft,
        last_value(region, true) over (partition by date_trunc('month', opened_date), city order by sqft, id rows between unbounded preceding and current row) last_known_region
    from kept
    where region in ('Central', 'North', 'East')
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from enriched s
    where not exists (
            select 1
            from inventory ch
            where ch.store_id = s.id
              and ch.last_restock >= '2023-04-01'
        )
)
select
    id,
    city,
    region,
    last3_min_sqft,
    last3_avg_sqft,
    last3_count_sqft,
    last3_sum_sqft,
    last_known_region
from eligible
order by last3_count_sqft nulls last
