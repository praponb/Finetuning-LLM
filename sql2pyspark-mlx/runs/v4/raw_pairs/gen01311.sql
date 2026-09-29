with filtered as (
    -- Filter the raw rows and derive helper columns
    select *, substring(method, 1, 3) as method_prefix
    from payments
    where order_id is not null
),
kept_keys as (
    -- Set difference on keys
    select order_id, method
    from filtered
    except
    select order_id, method
    from payments
    where method = 'promptpay'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered s
    inner join kept_keys k on s.order_id = k.order_id and s.method = k.method
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        round(amount / sum(amount) over (partition by order_id), 4) as amount_share,
        sum(amount) over w_part as amount_sum_90d
    from verified
    window w_part as (partition by method order by cast(paid_at as bigint) range between 7776000 preceding and current row)
),
with_dims as (
    -- Attach orders attributes
    select s.*, d.discount
    from metrics s
    left join orders d
        on s.order_id = d.id
),
summary as (
    -- Multi-level aggregation
    select
        date_trunc('month', paid_at) as activity_month,
        count(case when method = 'paypal' then 1 end) as paypal_count,
        sort_array(collect_set(method)) as method_list,
        count(case when status = 'SUCCESS' then 1 end) as success_count,
        count(*) as row_count,
        sum(case when status = 'FAILED' then amount_sum_90d else 0 end) as failed_amount_sum_90d,
        sum(case when status = 'REFUNDED' then amount_share else 0 end) as refunded_amount_share
    from with_dims
    group by date_trunc('month', paid_at)
)
select
    activity_month,
    method_list,
    row_count,
    refunded_amount_share
from summary
