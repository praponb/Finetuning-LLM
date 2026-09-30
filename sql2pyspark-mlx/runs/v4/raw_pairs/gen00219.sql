with base as (
    -- Keep recent, valid rows
    select *, lower(promo_code) as promo_code_lower
    from promotions
    where ends_on > date '2023-07-18'
      and channel is not null
)
-- Ranking and gap features
select
    id,
    channel,
    promo_code,
    starts_on,
    ends_on,
    discount_pct,
    promo_code_lower,
    percent_rank() over (partition by channel order by discount_pct desc) as discount_pct_percent_rank,
    avg(discount_pct) over (order by ends_on, id rows between 5 preceding and 1 preceding) as discount_pct_avg_5_prior,
    max(discount_pct) over (partition by promo_code) as promo_code_max_discount_pct,
    round(discount_pct / sum(discount_pct) over (partition by channel), 4) as discount_pct_share,
    avg(discount_pct) over w_grp as discount_pct_avg_2_prior
from base
window w_grp as (partition by promo_code order by ends_on, id rows between 2 preceding and 1 preceding)
