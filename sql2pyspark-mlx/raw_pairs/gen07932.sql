with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        k,
        value
    from carts
    lateral view explode(attributes) li as k, value
),
base as (
    -- Keep recent, valid rows
    select
        *,
        upper(k) as k_upper,
        coalesce(k, 'OTHER') as resolved_k
    from exploded
    where k in ('gift', 'utm_source')
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        k,
        value,
        k_upper,
        resolved_k,
        case when k = 'weight_kg' then 1 else 0 end as is_weight_kg,
        length(k) as k_length
    from base
    where k is not null
      and k is not distinct from 'device'
),
enriched as (
    -- Ranking and gap features
    select
        *,
        avg(is_weight_kg) over (order by k_length, id, k rows between 3 preceding and current row) as is_weight_kg_avg_3,
        first_value(k_length) ignore nulls over (partition by k order by k_length, id, k rows between unbounded preceding and current row) as first_known_k_length,
        first_value(k) ignore nulls over (partition by k order by k_length, id, k rows between unbounded preceding and current row) as first_known_k
    from filtered
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        is_weight_kg + k_length as is_weight_kg_plus_k_length,
        coalesce(k, 'OTHER') as k_clean
    from enriched
    where first_known_k_length >= 72
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        avg(is_weight_kg_avg_3) over (partition by k) as k_avg_is_weight_kg_avg_3,
        avg(is_weight_kg) over (partition by k order by is_weight_kg_avg_3, id, k rows between 5 preceding and 1 preceding) as is_weight_kg_avg_5_prior
    from cleaned
)
-- Ranking and gap features
select
    id,
    k,
    value,
    resolved_k,
    is_weight_kg_avg_5_prior,
    sum(is_weight_kg_avg_3) over (partition by k) as k_sum_is_weight_kg_avg_3,
    ntile(4) over (partition by k order by first_known_k_length, id, k) as first_known_k_length_bucket,
    ntile(10) over (partition by k order by k_avg_is_weight_kg_avg_3 desc, id, k) as k_avg_is_weight_kg_avg_3_bucket
from with_windows
where first_known_k_length between 27 and 52;
