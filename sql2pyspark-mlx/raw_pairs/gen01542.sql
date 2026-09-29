with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        store_id,
        order_date,
        status,
        amount,
        shipping_fee,
        amount * discount as amount_times_discount,
        coalesce(channel, 'N/A') as channel_filled
    from orders
    where shipping_fee between 11 and 24
),
cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        store_id,
        order_date,
        status,
        amount_times_discount,
        trim(status) as clean_status
    from filtered
    where order_date > date '2023-08-25'
      and store_id is not null
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, rank() over (partition by store_id order by amount_times_discount nulls last) as dedup_rank
        from cleaned
    ) t
    where dedup_rank = 1
),
enriched as (
    -- Ranking and gap features
    select
        id,
        order_date,
        status,
        amount_times_discount,
        clean_status,
        dedup_rank,
        avg(amount_times_discount) over (partition by store_id order by amount_times_discount, id rows between 6 preceding and current row) as last7_avg_amount_times_discount,
        max(amount_times_discount) over (partition by store_id order by amount_times_discount, id rows between 6 preceding and current row) as last7_max_amount_times_discount,
        min(amount_times_discount) over (partition by store_id order by amount_times_discount, id rows between 6 preceding and current row) as last7_min_amount_times_discount,
        avg(amount_times_discount) over (partition by month(order_date), store_id order by order_date, id rows between 6 preceding and current row) as amount_times_discount_avg_6
    from deduped
    where amount_times_discount < 39
)
select
    id,
    order_date,
    status,
    clean_status,
    last7_avg_amount_times_discount,
    last7_min_amount_times_discount,
    amount_times_discount_avg_6
from enriched
order by amount_times_discount_avg_6 desc;
