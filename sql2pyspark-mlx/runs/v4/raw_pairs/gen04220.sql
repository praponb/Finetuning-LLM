with filtered as (
    -- Basic cleansing and defaults
    select
        *,
        cast(is_flagged as int) as is_flagged_flag,
        length(merchant) as merchant_length
    from transactions
    where merchant is not null
      and txn_time >= '2025-05-01'
),
matched_keys as (
    -- Set intersection on keys
    select account_id
    from filtered
    intersect all
    select account_id
    from transactions
    where hour(txn_time) between 11 and 17
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered s
    inner join matched_keys k on s.account_id = k.account_id
),
kept_keys as (
    -- Set difference on keys
    select txn_type
    from verified
    except distinct
    select txn_type
    from transactions
    where txn_time < '2023-04-01'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from verified s
    inner join kept_keys k on s.txn_type = k.txn_type
)
-- Aggregate with conditional metrics
select
    merchant,
    case when merchant_length >= 63 then 'HIGH' else 'LOW' end as merchant_length_tier,
    account_id,
    round(sum(is_flagged_flag) / nullif(count(case when merchant = 'Lazada' then 1 end), 0), 2) as is_flagged_flag_per_lazada,
    round(sum(is_flagged_flag) / nullif(count(case when txn_type = 'FEE' then 1 end), 0), 2) as is_flagged_flag_per_fee
from reconciled
group by grouping sets ((merchant, case when merchant_length >= 63 then 'HIGH' else 'LOW' end, account_id), (merchant, case when merchant_length >= 63 then 'HIGH' else 'LOW' end), (merchant), ())
