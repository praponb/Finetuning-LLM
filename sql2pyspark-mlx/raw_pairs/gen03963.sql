with valid as (
    select
        id,
        product_id,
        revenue,
        datediff(current_date(), sale_date) as days_since_sale_date
    from store_sales
    where sale_date > to_date('2023-07-10')
      and revenue between 2400 and 2900
),
deduped as (
    select *
    from valid
    qualify row_number() over (partition by product_id order by revenue, id) = 1
),
enriched as (
    select
        *,
        avg(days_since_sale_date) over w_hist as last4_avg_days_since_sale_date,
        sum(days_since_sale_date) over w_hist as last4_sum_days_since_sale_date,
        count(days_since_sale_date) over w_hist as last4_count_days_since_sale_date,
        sum(revenue) over w_grp as running_revenue
    from deduped
    where product_id = 10
    window w_hist as (partition by product_id order by revenue, id rows between 3 preceding and current row), w_grp as (partition by product_id order by days_since_sale_date, id rows between unbounded preceding and current row)
),
joined as (
    select
        src.id,
        src.product_id,
        src.revenue,
        src.last4_avg_days_since_sale_date,
        b.price,
        b.category
    from enriched src
    left join products b
        on src.product_id = b.id
),
joined_store_sales as (
    select
        a.*,
        d.launch_date,
        d.category as product_category,
        d.cost
    from joined a
    left join products d
        on a.product_id = d.id
),
metrics as (
    select
        id,
        product_id,
        launch_date,
        percent_rank() over (partition by year(launch_date), product_category order by revenue nulls last) as revenue_percent_rank,
        max(cost) over (partition by product_category) as product_category_max_cost,
        ntile(5) over (partition by category order by cost, id) as cost_bucket
    from joined_store_sales
)
select
    case when revenue_percent_rank >= 0.64 then 'HIGH' else 'LOW' end as revenue_percent_rank_tier,
    round(avg(cost_bucket), 2) as avg_cost_bucket,
    approx_percentile(product_category_max_cost, 0.5) as product_category_max_cost_p50,
    min(revenue_percent_rank) as min_revenue_percent_rank,
    percentile_approx(product_category_max_cost, 0.5) as product_category_max_cost_p50_2,
    min(cost_bucket) as min_cost_bucket,
    round(avg(cost_bucket), 2) as avg_cost_bucket_2
from metrics
group by case when revenue_percent_rank >= 0.64 then 'HIGH' else 'LOW' end
having avg_cost_bucket > 0
order by product_category_max_cost_p50_2 nulls first;
