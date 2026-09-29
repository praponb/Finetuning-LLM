with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        pos,
        line.product_id,
        line.price
    from carts
    lateral view posexplode(items) t as pos, line
),
enriched as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        created_at,
        pos,
        product_id,
        price,
        lag(product_id, 2) over w_part as prev_product_id,
        ntile(5) over (partition by customer_id order by price desc, id, pos) as price_bucket,
        sum(pos) over (partition by customer_id) as customer_id_sum_pos
    from exploded
    window w_part as (partition by customer_id order by pos, id, pos)
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        round(prev_product_id / sum(prev_product_id) over (partition by customer_id), 4) as prev_product_id_share,
        first_value(pos) ignore nulls over (partition by customer_id order by price_bucket, id, pos rows between unbounded preceding and current row) as first_known_pos
    from enriched
)
select
    s.id,
    s.customer_id,
    s.created_at,
    s.pos,
    s.product_id,
    s.price,
    s.customer_id_sum_pos,
    s.prev_product_id_share,
    (select max(t2.balance) from accounts t2 where t2.customer_id = s.customer_id) as max_account_balance
from with_windows s
order by prev_product_id_share desc;
