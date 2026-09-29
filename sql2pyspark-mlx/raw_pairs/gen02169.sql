with with_latest as (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    select s.*, top1.sale_date
    from stores s
    left join lateral (
            select c.sale_date
            from store_sales c
            where c.store_id = s.id
            order by c.units desc, c.id desc
            limit 1
        ) top1 on true
),
deduped as (
    -- Keep one row per key (latest first)
    select
        id,
        store_name,
        city,
        region,
        sqft,
        sale_date
    from with_latest
    qualify row_number() over (partition by city order by opened_date desc, id) = 1
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        avg(sqft) over (partition by city order by sqft, id rows between 3 preceding and current row) last4_avg_sqft,
        sum(sqft) over (partition by city order by sqft, id rows between 3 preceding and current row) last4_sum_sqft,
        max(sqft) over (partition by city order by sqft, id rows between 3 preceding and current row) last4_max_sqft,
        row_number() over (order by sqft, id) event_seq
    from deduped
    where lower(store_name) like '%all%'
),
not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from metrics s
    where exists (
            select 1
            from orders ch
            where ch.store_id = s.id
              and ch.customer_id is null
        )
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from not_flagged s
    where not exists (
            select 1
            from store_sales ch
            where ch.store_id = s.id
              and ch.sale_date >= s.sale_date
        )
)
-- Running and moving aggregates
select
    id,
    store_name,
    region,
    sqft,
    sale_date,
    last4_avg_sqft,
    last4_max_sqft,
    row_number() over (partition by region order by last4_avg_sqft desc, id) seq_no,
    ntile(3) over (order by event_seq, id) event_seq_bucket
from kept;
