with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        k,
        attr_value
    from carts
    lateral view explode(attributes) t as k, attr_value
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        customer_id,
        created_at,
        k
    from (
        select *, row_number() over (partition by k, customer_id order by created_at, id, k) as latest_rank
        from exploded
    ) x
    where latest_rank = 1
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from deduped
    qualify row_number() over (partition by k, customer_id order by created_at desc, id, k) = 1
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        k,
        row_number() over (partition by k order by created_at, id, k) as row_seq,
        first_value(k) ignore nulls over (partition by k order by created_at, id, k rows between unbounded preceding and current row) as first_known_k,
        row_number() over (order by created_at desc, id, k) as seq_no
    from latest
)
-- Aggregate with conditional metrics
select
    case when row_seq >= 7 then 'PREMIUM' else 'STANDARD' end as row_seq_tier,
    customer_id,
    k,
    round(sum(row_seq) / nullif(count(case when k = 'utm_source' then 1 end), 0), 2) as row_seq_per_utm_source,
    sum(case when k = 'gift' then seq_no else 0 end) as gift_seq_no
from enriched
group by case when row_seq >= 7 then 'PREMIUM' else 'STANDARD' end, customer_id, k
order by customer_id desc;
