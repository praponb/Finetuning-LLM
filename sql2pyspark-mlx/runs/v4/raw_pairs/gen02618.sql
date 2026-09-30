with remaining_keys as (
    select product_id
    from order_items
    except
    select product_id
    from order_items
    where unit_price between 170 and 290
),
verified as (
    select s.*
    from order_items s
    inner join remaining_keys k on s.product_id = k.product_id
)
select
    case when quantity >= 7 then 'LARGE' else 'SMALL' end as quantity_tier,
    percentile_approx(quantity, array(0.25, 0.5, 0.75)) as quantity_quartiles,
    approx_percentile(unit_price, array(0.25, 0.5, 0.75)) as unit_price_quartiles,
    count(distinct product_id) as distinct_product_ids,
    approx_percentile(quantity, 0.95) as quantity_p95,
    approx_percentile(quantity, 0.5) as quantity_p50
from verified
group by rollup (case when quantity >= 7 then 'LARGE' else 'SMALL' end)
order by quantity_p95
