with lines as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        inline(items)
    from carts
)
-- Aggregate with conditional metrics
select
    case when qty >= 5 then 'HIGH' else 'LOW' end as qty_tier,
    round(avg(price), 2) as avg_price,
    round(avg(qty), 2) as avg_qty,
    percentile_approx(qty, array(0.25, 0.5, 0.75)) as qty_quartiles,
    count_if(promo = 'WELCOME5') as welcome5_events,
    approx_percentile(price, 0.95) as price_p95,
    sort_array(collect_set(promo)) as promo_list
from lines
group by case when qty >= 5 then 'HIGH' else 'LOW' end
having approx_percentile(price, 0.95) >= 0
