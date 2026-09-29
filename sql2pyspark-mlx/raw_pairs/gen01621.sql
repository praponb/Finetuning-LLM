with lines as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        li_item.product_id,
        li_item.qty,
        li_item.price,
        li_item.promo
    from carts
    lateral view explode(items) e as li_item
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        sum(product_id) over w_running as created_at_year_sum_product_id,
        dense_rank() over (partition by customer_id order by product_id desc nulls first) as product_id_dense_rank,
        avg(qty) over (partition by promo, customer_id order by cast(created_at as bigint) range between 900 preceding and current row) as qty_avg_15m
    from lines
    window w_running as (partition by year(created_at), promo)
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        qty,
        price,
        promo,
        created_at_year_sum_product_id,
        product_id_dense_rank,
        qty_avg_15m,
        greatest(product_id_dense_rank, qty) as greatest_product_id_dense_rank_qty,
        coalesce(promo, 'OTHER') as promo_clean
    from metrics
    where created_at >= date '2025-03-01'
      and customer_id is not null
),
enriched as (
    -- Attach customers attributes
    select
        a.*,
        b.credit_limit,
        b.signup_date,
        b.country
    from filtered a
    left join customers b
        on a.customer_id = b.id
),
kept_keys as (
    -- Set intersection on keys
    select id
    from enriched
    intersect
    select id
    from carts
    where created_at >= '2023-05-01'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    inner join kept_keys k on s.id = k.id
),
pivoted as (
    -- Native SQL PIVOT
    select *
    from (
        select
            promo,
            customer_id,
            country,
            created_at_year_sum_product_id
        from kept_rows
    ) src
    pivot (
        avg(created_at_year_sum_product_id) as value_sum
        for country in ('DE', 'UK')
    )
)
select
    promo,
    customer_id,
    DE,
    UK
from pivoted
order by DE desc;
