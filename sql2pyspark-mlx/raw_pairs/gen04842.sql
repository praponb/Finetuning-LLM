with enriched as (
    -- Window metrics over several different partitions
    select
        id,
        store_name,
        city,
        opened_date,
        sqft,
        lag(sqft, 1, 0) over (order by opened_date asc, id asc) as prev_sqft,
        max(sqft) over (partition by region) as region_max_sqft,
        round(sqft / sum(sqft) over (partition by region, city), 4) as sqft_share
    from stores
    where sqft >= 14775
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from enriched s
    where not exists (
            select 1
            from orders c
            where c.store_id = s.id
              and c.store_id = 40
        )
),
filtered as (
    -- Basic cleansing and defaults
    select *, sqft_share + region_max_sqft as sqft_share_plus_region_max_sqft
    from eligible
    where length(store_name) > 5
),
latest_match as (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    select s.*, top1.order_date
    from filtered s
    left join lateral (
            select c.order_date
            from orders c
            where c.store_id = s.id
              and c.order_date <= s.opened_date
            order by c.order_date desc, c.id desc
            limit 1
        ) top1 on true
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from latest_match s
    where exists (
            select 1
            from orders x
            where x.store_id = s.id
              and x.order_date < s.opened_date
        )
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        store_name,
        city,
        opened_date,
        sqft,
        region_max_sqft,
        order_date
    from (
        select *, row_number() over (partition by city order by sqft desc nulls first, id asc) as latest_rank
        from kept
    ) ranked
    where latest_rank = 1
)
select
    id,
    city,
    opened_date,
    sqft,
    region_max_sqft,
    case when city rlike '^(Singapore|London)$' then 'GROUP_B' when city rlike '^(Bangkok|Chiang Mai)$' then 'GROUP_A' else 'EXTENDED' end as city_zone
from ranked
order by city_zone desc nulls last
