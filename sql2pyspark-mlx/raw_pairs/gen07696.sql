with valid as (
    -- Keep recent, valid rows
    select *, add_months(ends_on, 12) as ends_on_plus_months
    from promotions
    where starts_on > date '2023-05-16'
)
-- Ranking and gap features
select
    id,
    discount_pct,
    ends_on_plus_months,
    lag(discount_pct, 1) over (partition by promo_code order by ends_on, id) as prev_discount_pct,
    avg(discount_pct) over (partition by year(ends_on_plus_months), promo_code order by ends_on, id rows between 2 preceding and 1 preceding) as discount_pct_avg_2_prior
from valid
where ends_on >= '2024-09-19'
