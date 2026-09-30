select *, row_number() over (partition by promo_code order by discount_pct, id) as rank_in_group
from promotions
qualify rank_in_group = 1
