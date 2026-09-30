with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        datediff(current_date(), ends_on) as days_since_ends_on
    from promotions
    where starts_on < '2023-09-01'
      and starts_on > date '2023-06-14'
)
-- Grouped metrics
select
    promo_code,
    count(distinct promo_code) as distinct_promo_codes,
    round(sum(days_since_ends_on) / nullif(count(case when promo_code = 'WELCOME5' then 1 end), 0), 2) as days_since_ends_on_per_welcome5,
    percentile_approx(days_since_ends_on, array(0.25, 0.5, 0.75)) as days_since_ends_on_quartiles
from filtered
group by promo_code;
