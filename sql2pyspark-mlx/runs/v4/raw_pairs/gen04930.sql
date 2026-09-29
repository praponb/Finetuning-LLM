with matched_keys as (
    -- Set difference on keys
    select id
    from payments
    except all
    select id
    from payments
    where status = 'REFUNDED'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from payments s
    join matched_keys k on s.id = k.id
),
with_dims as (
    -- Attach orders attributes
    select a.*, d.order_date
    from kept_rows a
    left join orders d
        on a.order_id = d.id
),
enriched as (
    -- Ranking and gap features
    select
        *,
        rank() over (order by amount desc nulls first) as amount_rank,
        dense_rank() over (partition by status, method order by amount) as amount_dense_rank
    from with_dims
),
joined as (
    -- Attach orders attributes
    select s.*, d.status as order_status
    from enriched s
    join orders d
        on s.order_id = d.id
)
-- Aggregate with conditional metrics
select
    method,
    order_id,
    count(*) filter (where order_status = 'COMPLETED') as completed_rows,
    count(distinct status) as distinct_status,
    sort_array(collect_set(order_status)) as order_status_list,
    sum(case when status = 'FAILED' then amount_rank else 0 end) as failed_amount_rank,
    grouping(method) as grouping_id
from joined
group by grouping sets ((method, order_id), (method))
order by method;
