with enriched as (
    select
        id,
        promo_code,
        starts_on,
        ends_on,
        min(discount_pct) over (partition by channel) as channel_min_discount_pct,
        rank() over (partition by month(starts_on), promo_code order by discount_pct) as discount_pct_rank,
        ntile(4) over (partition by channel order by discount_pct, id) as discount_pct_bucket
    from promotions
),
scored as (
    select
        id,
        starts_on,
        discount_pct_rank,
        discount_pct_bucket,
        avg(discount_pct_rank) over (partition by promo_code order by discount_pct_rank, id rows between 2 preceding and current row) as discount_pct_rank_avg_2,
        round(discount_pct_bucket / sum(discount_pct_bucket) over (partition by promo_code), 4) as discount_pct_bucket_share,
        avg(discount_pct_rank) over (partition by promo_code order by discount_pct_rank, id rows between 6 preceding and 1 preceding) as discount_pct_rank_avg_6_prior
    from enriched
    where starts_on > '2024-08-27'
),
cleaned as (
    select *, discount_pct_bucket_share * discount_pct_rank as discount_pct_bucket_share_times_discount_pct_rank
    from scored
    where starts_on > to_date('2023-05-24')
),
cleaned_promotions as (
    select
        id,
        starts_on,
        discount_pct_rank,
        discount_pct_rank_avg_2,
        discount_pct_bucket_share,
        discount_pct_rank_avg_6_prior,
        discount_pct_bucket_share_times_discount_pct_rank / nullif(discount_pct_bucket_share, 0) as discount_pct_bucket_share_times_discount_pct_rank_per_discount_pct_bucket_share
    from cleaned
    where starts_on >= date '2023-09-13'
),
with_windows as (
    select
        *,
        first_value(discount_pct_bucket_share, true) over (order by discount_pct_rank, id rows between unbounded preceding and current row) as first_known_discount_pct_bucket_share,
        first_value(discount_pct_rank, true) over (order by discount_pct_rank_avg_6_prior, id rows between unbounded preceding and current row) as first_known_discount_pct_rank,
        last_value(discount_pct_rank_avg_2, true) over (order by discount_pct_rank_avg_6_prior, id rows between unbounded preceding and current row) as last_known_discount_pct_rank_avg_2
    from cleaned_promotions
),
enriched_promotions as (
    select
        *,
        sum(first_known_discount_pct_rank) over (order by first_known_discount_pct_bucket_share, id rows between unbounded preceding and current row) as running_first_known_discount_pct_rank,
        ntile(4) over (order by discount_pct_bucket_share_times_discount_pct_rank_per_discount_pct_bucket_share desc, id) as discount_pct_bucket_share_times_discount_pct_rank_per_discount_pct_bucket_share_bucket,
        count(discount_pct_rank_avg_6_prior) over (partition by month(starts_on)) as starts_on_month_count_discount_pct_rank_avg_6_prior,
        last_value(discount_pct_bucket_share) ignore nulls over (order by first_known_discount_pct_rank, id rows between unbounded preceding and current row) as last_known_discount_pct_bucket_share
    from with_windows
),
valid as (
    select
        id,
        starts_on,
        discount_pct_rank,
        discount_pct_rank_avg_2,
        discount_pct_rank_avg_6_prior,
        discount_pct_bucket_share_times_discount_pct_rank_per_discount_pct_bucket_share,
        first_known_discount_pct_bucket_share,
        first_known_discount_pct_rank,
        last_known_discount_pct_rank_avg_2,
        running_first_known_discount_pct_rank,
        discount_pct_bucket_share_times_discount_pct_rank_per_discount_pct_bucket_share_bucket,
        round(discount_pct_rank / discount_pct_rank_avg_2, 2) as discount_pct_rank_per_discount_pct_rank_avg_2
    from enriched_promotions
    where starts_on >= date '2023-09-09'
)
select
    id,
    starts_on,
    discount_pct_rank,
    discount_pct_rank_avg_2,
    discount_pct_rank_avg_6_prior,
    first_known_discount_pct_bucket_share,
    last_known_discount_pct_rank_avg_2,
    running_first_known_discount_pct_rank,
    discount_pct_bucket_share_times_discount_pct_rank_per_discount_pct_bucket_share_bucket
from valid
order by id
limit 5;
