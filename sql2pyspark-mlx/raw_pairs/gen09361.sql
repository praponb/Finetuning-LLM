with base as (
    select
        *,
        add_months(starts_on, 1) as starts_on_plus_months,
        length(promo_code) as promo_code_length,
        coalesce(channel, 'OTHER') as channel_clean
    from promotions
    where channel is not null
      and discount_pct > 0.2
)
select
    id,
    channel,
    discount_pct,
    starts_on_plus_months,
    promo_code_length,
    lead(promo_code_length) over (partition by channel, promo_code order by starts_on, id) as next_promo_code_length,
    lag(promo_code_length) over (partition by channel, promo_code order by starts_on, id) as prev_promo_code_length,
    lag(discount_pct) over (partition by channel, promo_code order by starts_on, id) as prev_discount_pct,
    row_number() over (partition by channel, promo_code order by starts_on, id) as seq_no,
    count(discount_pct) over (partition by promo_code) as promo_code_count_discount_pct
from base
