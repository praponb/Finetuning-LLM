with cleaned as (
    select
        id,
        channel,
        promo_code,
        ends_on,
        length(promo_code) as promo_code_length,
        round(discount_pct * 0.9, 2) as discount_pct_adjusted
    from promotions
    where ends_on >= to_date('2023-08-01')
)
select
    *,
    min(discount_pct_adjusted) over (partition by promo_code order by ends_on, id rows between unbounded preceding and current row) as running_min_discount_pct_adjusted,
    avg(discount_pct_adjusted) over (partition by promo_code order by ends_on, id rows between unbounded preceding and current row) as running_avg_discount_pct_adjusted,
    count(discount_pct_adjusted) over (partition by promo_code order by ends_on, id rows between unbounded preceding and current row) as running_count_discount_pct_adjusted
from cleaned;
