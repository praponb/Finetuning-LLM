with enriched as (
    select
        *,
        ntile(3) over (partition by order_id, product_id order by unit_price, id) as unit_price_bucket,
        avg(quantity) over (order by quantity, id rows between 2 preceding and 1 preceding) as quantity_avg_2_prior,
        dense_rank() over (partition by product_id order by unit_price nulls first) as unit_price_dense_rank,
        ntile(5) over (partition by order_id order by unit_price, id) as unit_price_bucket_2,
        corr(quantity, unit_price) over (partition by product_id) as corr_quantity_unit_price
    from order_items
)
select
    s.id,
    s.order_id,
    s.product_id,
    s.quantity,
    s.unit_price,
    s.unit_price_dense_rank,
    s.unit_price_bucket_2,
    s.corr_quantity_unit_price,
    (select max(s2.revenue) from store_sales s2 where s2.product_id = s.product_id) as max_store_sale_revenue
from enriched s
