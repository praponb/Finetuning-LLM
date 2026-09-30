with flattened as (
    select
        id,
        effective_from,
        explode(tier_limits) as val
    from price_tiers
),
kept_keys as (
    select id
    from flattened
    except
    select id
    from price_tiers
    where plan in ('basic', 'pro', 'enterprise')
),
kept_rows as (
    select s.*
    from flattened s
    inner join kept_keys k on s.id = k.id
),
base as (
    select *, date_add(effective_from, 65) as effective_from_plus_65d
    from kept_rows
    where effective_from >= date '2024-06-02'
),
enriched as (
    select
        id,
        effective_from,
        dense_rank() over w_part as val_dense_rank,
        cume_dist() over w_recent as val_cume_dist,
        rank() over w_recent_2 as val_rank,
        dense_rank() over w_recent as val_dense_rank_2,
        dense_rank() over (order by val desc nulls last) as val_dense_rank_3
    from base
    window w_part as (order by val asc), w_recent as (order by val desc), w_recent_2 as (order by val asc nulls first)
)
select
    case when val_dense_rank_2 >= 7 then 'LARGE' else 'SMALL' end as val_dense_rank_2_tier,
    date_trunc('year', effective_from) as year_start,
    sum(val_rank) as total_val_rank,
    count(*) as num_rows,
    count(*) as cnt,
    min(val_rank) as min_val_rank
from enriched
group by 1, 2
having cnt > 1
