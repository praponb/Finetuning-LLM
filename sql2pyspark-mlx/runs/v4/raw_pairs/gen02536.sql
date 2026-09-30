with base as (
    -- Keep recent, valid rows
    select *, trim(method) clean_method
    from payments
    where paid_at >= timestamp '2024-01-13 07:00:00'
      and method is not null
      and method not in ('card', 'promptpay', 'bank_transfer')
),
enriched as (
    -- Ranking and gap features
    select
        *,
        dense_rank() over (order by amount desc nulls first) amount_dense_rank,
        avg(amount) over (order by paid_at asc, id asc rows between 5 preceding and current row) amount_avg_5,
        dense_rank() over (partition by order_id order by amount desc) amount_dense_rank_2,
        ntile(3) over (partition by method order by amount asc, id asc) amount_bucket
    from base
),
latest as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by status order by amount asc nulls first, id asc) as dedup_rank
        from enriched
    ) ranked
    where dedup_rank = 1
)
-- Grouped metrics
select
    case when amount_avg_5 >= 1662 then 'LARGE' else 'SMALL' end as amount_avg_5_tier,
    (unix_timestamp(max(paid_at)) - unix_timestamp(min(paid_at))) / 60 as duration_min,
    sort_array(collect_set(method)) as method_list,
    min(amount_dense_rank) as min_amount_dense_rank,
    count(distinct id) as distinct_ids
from latest
group by case when amount_avg_5 >= 1662 then 'LARGE' else 'SMALL' end
order by min_amount_dense_rank asc nulls first;
