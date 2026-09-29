with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when is_flagged then 'Yes' else 'No' end as is_flagged_label,
        cast(is_flagged as int) as is_flagged_flag,
        coalesce(merchant, 'N/A') as resolved_merchant
    from transactions
    where txn_time > timestamp '2024-04-06 14:00:00'
      and merchant is not null
      and amount between 1400 and 1900
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        avg(amount) over (partition by txn_type order by txn_time, id rows between 3 preceding and current row) as amount_avg_3,
        ntile(4) over (order by amount desc, id) as amount_bucket,
        last_value(merchant) ignore nulls over (partition by merchant, txn_type order by amount, id rows between unbounded preceding and current row) as last_known_merchant
    from cleaned
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from scored
    qualify row_number() over (partition by merchant, account_id order by amount_avg_3, id) = 1
),
cleaned_transactions as (
    -- Basic cleansing and defaults
    select
        *,
        greatest(amount, amount_bucket) as greatest_amount_amount_bucket,
        lower(merchant) as merchant_lower
    from latest
    where txn_time >= to_date('2023-08-26')
      and merchant is not null
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        merchant,
        greatest_amount_amount_bucket,
        amount
    from cleaned_transactions
) src
unpivot include nulls (
    reading for metric in (greatest_amount_amount_bucket as GREATEST_AMOUNT_AMOUNT_BUCKET, amount as AMOUNT)
);
