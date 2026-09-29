with valid as (
    -- Keep recent, valid rows
    select
        id,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        date_sub(starts_on, 9) as starts_on_minus_9d,
        coalesce(channel, 'N/A') as channel_clean
    from promotions
    where channel is not null
)
-- Running and moving aggregates
select
    *,
    rank() over w_recent as discount_pct_rank,
    dense_rank() over w_recent as discount_pct_dense_rank,
    sum(discount_pct) over (partition by promo_code order by starts_on, id rows between unbounded preceding and current row) as cumulative_discount_pct
from valid
where ends_on < '2025-06-24'
window w_recent as (partition by promo_code order by discount_pct);
