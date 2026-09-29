with with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        sum(discount_pct) over w_hist last7_sum_discount_pct,
        max(discount_pct) over w_hist last7_max_discount_pct,
        min(discount_pct) over w_hist last7_min_discount_pct,
        count(discount_pct) over w_hist last7_count_discount_pct
    from promotions
    window w_hist as (partition by channel order by starts_on, id rows between 6 preceding and current row)
)
select
    id,
    channel,
    promo_code,
    starts_on,
    last7_max_discount_pct,
    last7_min_discount_pct,
    last7_count_discount_pct,
    case when promo_code rlike '^(BUNDLE|WELCOME5)$' then 'GROUP_B' when promo_code rlike '^(SPRING10|VIP)$' then 'PRIORITY' else 'OTHER_ZONE' end as promo_code_zone
from with_windows
order by promo_code nulls first, last7_count_discount_pct;
