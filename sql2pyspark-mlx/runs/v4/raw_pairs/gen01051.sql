with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        elem.price,
        elem.promo
    from carts
    lateral view outer explode(items) li as elem
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        dense_rank() over (order by price desc) as price_dense_rank,
        round(price / sum(price) over (partition by customer_id), 4) as price_share,
        round(price / sum(price) over (partition by promo), 4) as price_share_2,
        percent_rank() over w_ordered as price_percent_rank
    from exploded
    window w_ordered as (order by price desc nulls first)
),
metrics_carts as (
    -- Running and moving aggregates
    select
        *,
        sum(price_dense_rank) over (partition by customer_id) as customer_id_sum_price_dense_rank,
        rank() over (partition by promo, customer_id order by price_percent_rank desc) as price_percent_rank_rank,
        stddev_samp(price_share_2) over (partition by customer_id) as customer_id_stddev_samp_price_share_2,
        percent_rank() over (partition by customer_id order by price_share_2 desc nulls first) as price_share_2_percent_rank
    from metrics
),
enriched as (
    -- Ranking and gap features
    select
        *,
        percent_rank() over (order by price_dense_rank desc) as price_dense_rank_percent_rank,
        percent_rank() over (partition by customer_id order by customer_id_sum_price_dense_rank nulls first) as customer_id_sum_price_dense_rank_percent_rank,
        percent_rank() over (partition by customer_id, promo order by price desc nulls first) as price_percent_rank_2
    from metrics_carts
)
-- Aggregate with conditional metrics
select
    case when customer_id_sum_price_dense_rank >= 44 then 'LARGE' else 'SMALL' end as customer_id_sum_price_dense_rank_tier,
    count_if(promo = 'SPRING10') as spring10_events,
    size(collect_set(promo)) as num_promos,
    count_if(promo = 'WELCOME5') as welcome5_events
from enriched
group by case when customer_id_sum_price_dense_rank >= 44 then 'LARGE' else 'SMALL' end
order by customer_id_sum_price_dense_rank_tier desc nulls first;
