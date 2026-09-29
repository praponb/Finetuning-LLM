with matched as (
    -- Non-equi join on a validity window
    select
        src.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from orders src
    left join subscriptions sub
        on src.customer_id = sub.customer_id
       and src.order_date between sub.start_date and sub.end_date
),
scored as (
    -- Running and moving aggregates
    select
        *,
        dense_rank() over (partition by status, store_id order by discount desc nulls first) as discount_dense_rank,
        avg(monthly_fee) over (partition by status order by monthly_fee, id, subscription_id rows between 5 preceding and current row) as monthly_fee_avg_5,
        round(subscription_id / sum(subscription_id) over (partition by date_trunc('month', order_date), channel), 4) as subscription_id_share,
        last_value(channel) ignore nulls over (partition by customer_id order by order_date, id, subscription_id rows between unbounded preceding and current row) as last_known_channel
    from matched
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        last_value(channel, true) over (partition by store_id order by discount_dense_rank, id, subscription_id rows between unbounded preceding and current row) as last_known_channel_2,
        lag(amount, 2) over (partition by date_trunc('MONTH', order_date), plan order by amount, id, subscription_id) as prev_amount,
        avg(subscription_id) over (order by order_date, id, subscription_id rows between 5 preceding and 1 preceding) as subscription_id_avg_5_prior
    from scored
    where subscription_id_share > 0.64
),
metrics as (
    -- Ranking and gap features
    select
        *,
        avg(discount_dense_rank) over w_hist as discount_dense_rank_avg_3_prior,
        row_number() over w_ordered as row_seq
    from enriched
    window w_hist as (partition by date_trunc('WEEK', order_date) order by subscription_id_avg_5_prior, id, subscription_id rows between 3 preceding and 1 preceding), w_ordered as (partition by customer_id, store_id order by subscription_id_share desc, id, subscription_id)
)
-- Aggregate with conditional metrics
select
    status,
    customer_id,
    date_trunc('week', order_date) as week_start,
    count(*) filter (where status = 'RETURNED') as returned_rows,
    sort_array(collect_set(channel)) as channel_list,
    sum(case when status = 'PENDING' then discount else 0 end) as pending_discount,
    approx_percentile(subscription_id_share, 0.9) as subscription_id_share_p90
from metrics
group by status, customer_id, week_start
order by status
