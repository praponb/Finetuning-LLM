with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        txn_type,
        merchant,
        if(is_flagged, 'Yes', 'No') as is_flagged_label,
        initcap(trim(merchant)) as merchant_title,
        coalesce(merchant, 'OTHER') as merchant_clean
    from transactions
    where merchant is not null
),
valid as (
    -- Keep recent, valid rows
    select
        *,
        upper(merchant) as merchant_upper,
        concat_ws('-', txn_type, merchant) as txn_type_merchant_key
    from base
    where merchant is not null
      and txn_type in ('CREDIT', 'FEE')
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, upper(merchant) as merchant_upper_2
    from valid
    where merchant is not null
      and txn_type in ('CREDIT', 'DEBIT', 'FEE')
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        txn_type,
        merchant,
        is_flagged_label,
        merchant_title,
        merchant_clean,
        merchant_upper,
        merchant_upper_2,
        trim(merchant) as clean_merchant
    from cleaned
    where merchant is not distinct from 'Grab'
),
filtered_transactions as (
    -- Keep recent, valid rows
    select
        id,
        txn_type,
        merchant,
        is_flagged_label,
        merchant_title,
        merchant_clean,
        merchant_upper,
        merchant_upper_2,
        clean_merchant,
        substring(merchant, 1, 4) as merchant_prefix,
        coalesce(merchant, 'NONE') as merchant_clean_2
    from filtered
    where merchant in ('Grab', 'Lazada', 'Shell')
),
base_transactions as (
    -- Basic cleansing and defaults
    select
        id,
        txn_type,
        merchant,
        is_flagged_label,
        merchant_title,
        merchant_clean,
        merchant_upper,
        merchant_upper_2,
        merchant_clean_2,
        length(txn_type) as txn_type_length,
        lower(txn_type) as txn_type_lower,
        coalesce(merchant, 'UNKNOWN') as merchant_filled
    from filtered_transactions
    where merchant is not null
)
select
    id,
    txn_type,
    merchant,
    is_flagged_label,
    merchant_title,
    merchant_upper,
    merchant_upper_2,
    merchant_filled
from base_transactions
