select
    *,
    row_number() over w_running as event_seq,
    sum(discount_pct) over (partition by channel order by starts_on, id rows between unbounded preceding and current row) as cumulative_discount_pct,
    round(discount_pct / sum(discount_pct) over (partition by promo_code), 4) as discount_pct_share,
    row_number() over (partition by channel order by ends_on desc, id) as seq_no,
    last_value(channel, true) over (partition by promo_code order by ends_on, id rows between unbounded preceding and current row) as last_known_channel
from promotions
window w_running as (order by discount_pct desc, id);
