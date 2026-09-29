with lines as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        pos + 1 as line_no,
        line.product_id,
        line.qty,
        line.price,
        line.promo
    from carts
    lateral view posexplode(items) li as pos, line
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from lines
    qualify row_number() over (partition by customer_id order by price asc nulls last, id asc, line_no asc) = 1
),
ranked as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by customer_id, promo order by line_no desc, id asc, line_no asc) as row_num
    from latest
    qualify row_num = 1
),
cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        round(qty / price, 2) as qty_per_price,
        coalesce(promo, 'DIRECT') as resolved_promo
    from ranked
    where promo is not null
),
enriched as (
    -- Attach customers attributes
    select s.*, b.email
    from cleaned s
    inner join customers b
        on s.customer_id = b.id
    where b.country in ('DE', 'UK')
),
with_dims as (
    -- Attach customers attributes
    select
        s.*,
        b.email as customer_email,
        b.credit_limit
    from enriched s
    left join customers b
        on s.customer_id = b.id
),
latest_carts as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, rank() over (partition by customer_id order by qty_per_price desc) as latest_rank
        from with_dims
    ) x
    where latest_rank = 1
)
select
    id,
    line_no,
    product_id,
    qty,
    price,
    promo,
    row_num,
    resolved_promo,
    email
from latest_carts
order by line_no desc nulls last;
