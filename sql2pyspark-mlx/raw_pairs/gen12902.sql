with unpacked as (
    -- Semi-structured parsing
    select
        id,
        coupon_codes,
        ship_to,
        attributes,
        ship_to.city as ship_city,
        ship_to.country as ship_country,
        size(items) as num_items
    from carts
)
-- Window metrics over several different partitions
select
    id,
    ship_country,
    num_items,
    row_number() over (partition by ship_country order by num_items desc, id) row_seq,
    rank() over (partition by ship_country order by num_items desc) num_items_rank,
    avg(num_items) over (partition by ship_country order by num_items, id rows between 6 preceding and current row) num_items_avg_6,
    last_value(num_items) ignore nulls over (partition by ship_country order by num_items, id rows between unbounded preceding and current row) last_known_num_items
from unpacked
where size(coupon_codes) >= 2
