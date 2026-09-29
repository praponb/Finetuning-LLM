with exploded as (
    -- Unnest the array into one row per element
    select
        src.id,
        src.customer_id,
        src.created_at,
        pos,
        line.product_id,
        line.qty,
        line.price,
        line.promo
    from carts src
    lateral view posexplode(src.items) t as pos, line
)
-- Ranking and gap features
select
    id,
    created_at,
    pos,
    qty,
    promo,
    max(price) over w_part as last7_max_price,
    min(price) over w_part as last7_min_price,
    avg(price) over w_part as last7_avg_price,
    count(price) over w_part as last7_count_price
from exploded
window w_part as (partition by customer_id, promo order by price, id, pos rows between 6 preceding and current row)
