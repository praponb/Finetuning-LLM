with with_windows as (
    select
        id,
        channel,
        promo_code,
        starts_on,
        max(discount_pct) over (partition by promo_code, channel) as promo_code_max_discount_pct,
        avg(discount_pct) over (partition by channel order by discount_pct, id rows between 5 preceding and 1 preceding) as discount_pct_avg_5_prior,
        ntile(10) over (partition by promo_code order by discount_pct desc, id) as discount_pct_bucket,
        avg(discount_pct) over (partition by month(ends_on), promo_code order by starts_on, id rows between 5 preceding and current row) as discount_pct_avg_5
    from promotions
    where month(starts_on) in (3, 4, 10)
)
select
    id,
    channel,
    promo_code,
    starts_on,
    promo_code_max_discount_pct,
    discount_pct_avg_5_prior,
    discount_pct_bucket,
    discount_pct_avg_5
from with_windows
