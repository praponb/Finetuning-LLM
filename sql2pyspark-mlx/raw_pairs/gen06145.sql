with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        trim(channel) as clean_channel,
        coalesce(channel, 'OTHER') as resolved_channel
    from promotions
    where channel is not null
),
with_window as (
    -- Non-equi join on a validity window
    select
        e.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from cleaned e
    inner join promotions pr
        on e.channel <=> pr.channel
       and e.ends_on >= pr.starts_on
       and e.ends_on < pr.ends_on + interval 14 days
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        first_value(promotion_promo_code, true) over (order by discount_pct, id, promo_id rows between unbounded preceding and current row) as first_known_promotion_promo_code,
        ntile(4) over (order by discount_pct, id, promo_id) as discount_pct_bucket,
        dense_rank() over (partition by promo_code order by discount_pct desc) as discount_pct_dense_rank,
        ntile(4) over (partition by channel order by promotion_discount_pct desc, id, promo_id) as promotion_discount_pct_bucket,
        rank() over (partition by channel order by promo_id desc) as promo_id_rank
    from with_window
),
matched_keys as (
    -- Set difference on keys
    select promo_code
    from with_windows
    except
    select promo_code
    from promotions
    where discount_pct <= 0.29
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_windows s
    inner join matched_keys k on s.promo_code = k.promo_code
),
totals as (
    -- Aggregate with conditional metrics
    select
        channel,
        case when discount_pct >= 0.26 then 'PREMIUM' else 'STANDARD' end as discount_pct_tier,
        size(collect_set(promotion_promo_code)) as num_promotion_promo_codes,
        count(*) as row_count,
        sort_array(collect_set(channel)) as channel_list,
        rank() over (partition by channel order by size(collect_set(promotion_promo_code)) desc) as num_promotion_promo_codes_rank
    from reconciled
    group by channel, case when discount_pct >= 0.26 then 'PREMIUM' else 'STANDARD' end
    having num_promotion_promo_codes >= 1
)
select
    channel,
    discount_pct_tier,
    num_promotion_promo_codes,
    row_count,
    channel_list,
    num_promotion_promo_codes_rank
from totals
order by num_promotion_promo_codes
