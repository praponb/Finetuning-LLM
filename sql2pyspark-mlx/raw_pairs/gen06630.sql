with valid as (
    -- Basic cleansing and defaults
    select
        id,
        store_name,
        city,
        opened_date,
        sqft,
        datediff(current_date(), opened_date) as days_since_opened_date
    from stores
    where year(opened_date) >= 2024
      and region is distinct from 'South'
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        ntile(10) over (partition by city order by sqft desc, id) as sqft_bucket,
        lead(days_since_opened_date, 1) over w_ordered as next_days_since_opened_date,
        rank() over (partition by city order by sqft desc) as sqft_rank,
        round(sqft / sum(sqft) over (partition by city), 4) as sqft_share
    from valid
    window w_ordered as (partition by city order by opened_date, id)
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from metrics s
    where not exists (
            select 1
            from inventory x
            where x.store_id = s.id
              and x.last_restock <= s.opened_date
        )
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        dense_rank() over (partition by date_trunc('WEEK', opened_date), city order by days_since_opened_date desc) as days_since_opened_date_dense_rank,
        avg(sqft_bucket) over (partition by city order by sqft, id rows between 6 preceding and 1 preceding) as sqft_bucket_avg_6_prior,
        first_value(sqft_bucket) ignore nulls over (partition by city order by sqft, id rows between unbounded preceding and current row) as first_known_sqft_bucket,
        sum(sqft) over (partition by month(opened_date), city order by sqft_share, id rows between unbounded preceding and current row) as running_sqft
    from eligible
    where opened_date >= '2025-05-04'
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from enriched s
    where not exists (
            select 1
            from store_sales ch
            where ch.store_id = s.id
              and ch.sale_date <= s.opened_date
        )
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by city order by sqft_bucket_avg_6_prior, id) as rank_in_group
        from not_flagged
    ) ranked
    where rank_in_group = 1
),
with_latest as (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.quantity,
        latest.inventory_last_restock
    from ranked s
    left join lateral (
            select c.quantity, c.last_restock as inventory_last_restock
            from inventory c
            where c.store_id = s.id
            order by c.last_restock desc, c.id desc
            limit 1
        ) latest on true
)
select
    id,
    city,
    sqft,
    days_since_opened_date,
    sqft_bucket,
    sqft_rank,
    sqft_share,
    days_since_opened_date_dense_rank,
    sqft_bucket_avg_6_prior,
    case when city rlike '^(Tokyo|Berlin)$' then 'EXTENDED' when city rlike '^(Bangkok|Chiang Mai)$' then 'GROUP_A' else 'CORE' end as city_zone
from with_latest
