select
    *,
    dense_rank() over (order by discount_pct) as discount_pct_dense_rank,
    round(discount_pct / sum(discount_pct) over (partition by promo_code), 4) as discount_pct_share,
    row_number() over (order by starts_on desc, id) as row_seq
from promotions;
