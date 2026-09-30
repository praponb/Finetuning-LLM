with filtered as (
    select
        *,
        cast(is_flagged as int) as is_flagged_flag,
        coalesce(merchant, 'NONE') as resolved_merchant
    from transactions
    where merchant is distinct from 'Amazon'
),
matched_keys as (
    select id, merchant
    from filtered
    intersect all
    select id, merchant
    from transactions
    where merchant <> 'Grab'
),
kept_rows as (
    select s.*
    from filtered s
    join matched_keys k on s.id = k.id and s.merchant = k.merchant
),
base as (
    select
        account_id,
        merchant,
        is_flagged_flag,
        resolved_merchant,
        greatest(amount, is_flagged_flag) as greatest_amount_is_flagged_flag,
        concat_ws('-', txn_type, merchant) as txn_type_merchant_key
    from kept_rows
    where txn_time >= timestamp '2023-06-28 16:00:00'
),
metrics as (
    select
        merchant,
        resolved_merchant,
        rank() over (partition by account_id order by greatest_amount_is_flagged_flag nulls first) as greatest_amount_is_flagged_flag_rank,
        max(is_flagged_flag) over (partition by merchant) as merchant_max_is_flagged_flag,
        max(greatest_amount_is_flagged_flag) over (partition by account_id) as account_id_max_greatest_amount_is_flagged_flag,
        cume_dist() over (partition by merchant order by greatest_amount_is_flagged_flag) as greatest_amount_is_flagged_flag_cume_dist,
        sum(is_flagged_flag) over (partition by merchant) as merchant_sum_is_flagged_flag
    from base
),
base_transactions as (
    select
        merchant,
        resolved_merchant,
        greatest_amount_is_flagged_flag_rank,
        greatest_amount_is_flagged_flag_cume_dist,
        round(merchant_max_is_flagged_flag * 0.9, 2) as merchant_max_is_flagged_flag_scaled,
        coalesce(merchant, 'OTHER') as merchant_clean
    from metrics
    where merchant_max_is_flagged_flag between 21 and 38
),
scored as (
    select
        *,
        dense_rank() over (partition by merchant order by greatest_amount_is_flagged_flag_rank desc) as greatest_amount_is_flagged_flag_rank_dense_rank,
        round(greatest_amount_is_flagged_flag_rank / sum(greatest_amount_is_flagged_flag_rank) over (partition by merchant), 4) as greatest_amount_is_flagged_flag_rank_share,
        cume_dist() over (partition by merchant order by greatest_amount_is_flagged_flag_cume_dist desc nulls last) as greatest_amount_is_flagged_flag_cume_dist_cume_dist
    from base_transactions
)
select
    merchant,
    max(merchant_max_is_flagged_flag_scaled) as max_merchant_max_is_flagged_flag_scaled,
    round(avg(greatest_amount_is_flagged_flag_cume_dist_cume_dist), 2) as avg_greatest_amount_is_flagged_flag_cume_dist_cume_dist,
    count(*) filter (where merchant = 'Starbucks') as starbucks_rows,
    grouping(merchant) as grouping_id
from scored
group by rollup (merchant)
having count(*) filter (where merchant = 'Starbucks') >= 1
order by merchant nulls first;
