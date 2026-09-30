with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        order_date,
        status,
        amount,
        discount,
        shipping_fee,
        least(amount, shipping_fee) as least_amount_shipping_fee,
        round(discount / amount, 2) as discount_per_amount
    from orders
    where order_date >= date '2023-08-01'
      and amount between 1300 and 1440
),
scored as (
    -- Ranking and gap features
    select
        id,
        amount,
        discount,
        least_amount_shipping_fee,
        discount_per_amount,
        lag(least_amount_shipping_fee) over (partition by status order by discount_per_amount, id) as prev_least_amount_shipping_fee,
        lead(amount) over (partition by status order by discount_per_amount, id) as next_amount,
        lag(amount) over (partition by status order by discount_per_amount, id) as prev_amount,
        row_number() over (partition by status order by discount_per_amount, id) as event_seq,
        lag(discount_per_amount, 1) over (order by amount, id) as prev_discount_per_amount
    from cleaned
),
enriched as (
    -- Ranking and gap features
    select
        id,
        discount_per_amount,
        next_amount,
        lag(prev_discount_per_amount) over (order by least_amount_shipping_fee, id) as prev_prev_discount_per_amount,
        lead(least_amount_shipping_fee) over (order by least_amount_shipping_fee, id) as next_least_amount_shipping_fee,
        lag(least_amount_shipping_fee) over (order by least_amount_shipping_fee, id) as prev_least_amount_shipping_fee_2,
        row_number() over (order by least_amount_shipping_fee, id) as row_seq,
        avg(discount_per_amount) over (order by prev_least_amount_shipping_fee, id rows between 3 preceding and current row) as discount_per_amount_avg_3
    from scored
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        discount_per_amount,
        next_amount,
        prev_least_amount_shipping_fee_2,
        row_seq,
        discount_per_amount_avg_3,
        cast(discount_per_amount_avg_3 as string) as discount_per_amount_avg_3_str,
        next_amount / nullif(discount_per_amount, 0) as next_amount_per_discount_per_amount
    from enriched
    where next_amount <= 1580
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        discount_per_amount,
        prev_least_amount_shipping_fee_2,
        row_seq,
        discount_per_amount_avg_3,
        discount_per_amount_avg_3_str,
        next_amount_per_discount_per_amount,
        discount_per_amount * row_seq as discount_per_amount_times_row_seq,
        least(row_seq, discount_per_amount_avg_3) as least_row_seq_discount_per_amount_avg_3
    from valid
    where next_amount_per_discount_per_amount is not null
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        discount_per_amount_times_row_seq,
        prev_least_amount_shipping_fee_2,
        next_amount_per_discount_per_amount
    from base
) src
unpivot (
    val for metric_name in (discount_per_amount_times_row_seq, prev_least_amount_shipping_fee_2 as PREV_LEAST_AMOUNT_SHIPPING_FEE_2_VAL, next_amount_per_discount_per_amount as NEXT_AMOUNT_PER_DISCOUNT_PER_AMOUNT)
)
