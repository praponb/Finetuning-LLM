with filtered as (
    select
        *,
        date_trunc('month', paid_at) paid_at_period,
        trim(method) clean_method,
        coalesce(method, 'N/A') method_clean
    from payments
    where paid_at > date '2023-06-11'
      and order_id is not null
),
metrics as (
    select
        id,
        order_id,
        method,
        amount,
        paid_at,
        clean_method,
        avg(amount) over (partition by status order by cast(paid_at as bigint) range between 2592000 preceding and current row) amount_avg_30d,
        count(amount) over (partition by order_id) order_id_count_amount
    from filtered
),
valid as (
    select
        id,
        order_id,
        method,
        paid_at,
        clean_method,
        amount_avg_30d,
        order_id_count_amount,
        datediff(current_date(), paid_at) / 7 weeks_since_paid_at
    from metrics
    where paid_at >= to_date('2023-10-05')
      and amount < 420
),
metrics_payments as (
    select
        *,
        avg(order_id_count_amount) over (partition by order_id order by amount_avg_30d, id rows between 3 preceding and 1 preceding) order_id_count_amount_avg_3_prior,
        sum(amount_avg_30d) over (partition by order_id order by order_id_count_amount, id rows between unbounded preceding and current row) running_amount_avg_30d,
        dense_rank() over (order by amount_avg_30d desc) amount_avg_30d_dense_rank,
        ntile(4) over (order by order_id_count_amount, id) order_id_count_amount_bucket,
        row_number() over (partition by method, order_id order by amount_avg_30d, id) seq_no
    from valid
)
select
    id,
    order_id,
    paid_at,
    weeks_since_paid_at,
    order_id_count_amount_avg_3_prior,
    running_amount_avg_30d,
    order_id_count_amount_bucket,
    seq_no
from metrics_payments
order by id nulls last;
