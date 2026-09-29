with matched as (
    -- Non-equi join on a validity window
    select
        a.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from promotions a
    left join promotions pr
        on coalesce(a.channel, 'store') = pr.channel
       and a.ends_on >= pr.starts_on
       and a.ends_on < pr.ends_on + interval '7' day
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from matched
    qualify row_number() over (partition by promo_code order by promo_id desc nulls first, id, promo_id) = 1
),
remaining_keys as (
    -- Set intersection on keys
    select promo_code
    from deduped
    intersect
    select promo_code
    from promotions
    where channel is null
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from deduped s
    join remaining_keys k on s.promo_code = k.promo_code
),
metrics as (
    -- Ranking and gap features
    select
        *,
        dense_rank() over (partition by year(ends_on) order by promotion_discount_pct desc nulls last) promotion_discount_pct_dense_rank,
        count(promo_id) over (partition by promo_code, channel) promo_code_count_promo_id,
        round(discount_pct / sum(discount_pct) over (partition by month(ends_on), promotion_promo_code), 4) discount_pct_share,
        round(promo_id / sum(promo_id) over (partition by promotion_promo_code), 4) promo_id_share,
        count(discount_pct) over (partition by year(starts_on), promo_code) starts_on_year_count_discount_pct
    from reconciled
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        rank() over (partition by month(starts_on), promotion_promo_code order by promo_id_share desc) promo_id_share_rank,
        round(promo_code_count_promo_id / sum(promo_code_count_promo_id) over (partition by promotion_promo_code), 4) promo_code_count_promo_id_share,
        sum(promotion_discount_pct_dense_rank) over (partition by month(starts_on), channel) starts_on_month_sum_promotion_discount_pct_dense_rank,
        round(discount_pct / sum(discount_pct) over (partition by channel), 4) discount_pct_share_2
    from metrics
    where promo_id is null
),
with_windows_promotions as (
    -- Ranking and gap features
    select
        id,
        promo_code,
        promotion_promo_code,
        promotion_discount_pct,
        promotion_discount_pct_dense_rank,
        promo_code_count_promo_id,
        promo_id_share,
        promo_code_count_promo_id_share,
        starts_on_month_sum_promotion_discount_pct_dense_rank,
        discount_pct_share_2,
        min(promotion_discount_pct) over (partition by date_trunc('YEAR', ends_on), promo_code) ends_on_period_min_promotion_discount_pct,
        min(discount_pct) over w_part channel_min_discount_pct,
        percent_rank() over (partition by channel order by discount_pct) discount_pct_percent_rank,
        avg(promo_id_share_rank) over (partition by promo_code, channel) promo_code_avg_promo_id_share_rank
    from with_windows
    window w_part as (partition by channel)
),
with_windows_promotions_2 as (
    -- Running and moving aggregates
    select
        id,
        promo_code_count_promo_id,
        promo_id_share,
        starts_on_month_sum_promotion_discount_pct_dense_rank,
        discount_pct_share_2,
        discount_pct_percent_rank,
        rank() over (order by promo_code_count_promo_id_share desc nulls last) promo_code_count_promo_id_share_rank,
        max(discount_pct_percent_rank) over (partition by promo_code) promo_code_max_discount_pct_percent_rank
    from with_windows_promotions
)
-- Multi-level aggregation
select
    case when promo_id_share >= 0.86 then 'PREMIUM' else 'STANDARD' end as promo_id_share_tier,
    round(avg(discount_pct_share_2), 2) as avg_discount_pct_share_2,
    percentile_approx(starts_on_month_sum_promotion_discount_pct_dense_rank, array(0.25, 0.5, 0.75)) as starts_on_month_sum_promotion_discount_pct_dense_rank_quartiles,
    count(*) as row_count,
    count(distinct id) as distinct_ids,
    approx_percentile(promo_code_count_promo_id_share_rank, 0.95) as promo_code_count_promo_id_share_rank_p95,
    approx_percentile(starts_on_month_sum_promotion_discount_pct_dense_rank, array(0.25, 0.5, 0.75)) as starts_on_month_sum_promotion_discount_pct_dense_rank_quartiles_2
from with_windows_promotions_2
group by cube (case when promo_id_share >= 0.86 then 'PREMIUM' else 'STANDARD' end)
having row_count > 1
order by row_count
