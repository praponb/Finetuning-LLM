with with_dims as (
    -- Attach stores attributes
    select
        s.*,
        b.region,
        b.sqft
    from store_sales s
    left join stores b
        on s.store_id = b.id
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        round(revenue / sum(revenue) over (partition by store_id), 4) as revenue_share,
        row_number() over (partition by store_id order by units, id) as row_seq,
        row_number() over (partition by region, store_id order by revenue, id) as seq_no,
        sum(revenue) over (partition by product_id order by sale_date, id rows between unbounded preceding and current row) as cumulative_revenue
    from with_dims
),
joined as (
    -- Attach products attributes
    select
        a.*,
        b.price,
        b.category,
        b.stock_qty
    from scored a
    left join products b
        on a.product_id = b.id
)
-- Multi-level aggregation
select
    case when row_seq >= 7 then 'LARGE' else 'SMALL' end as row_seq_tier,
    store_id,
    case when revenue_share >= 0.72 then 'HIGH' else 'LOW' end as revenue_share_tier,
    avg(price) filter (where category = 'Home') as home_avg_price,
    min(sqft) as min_sqft,
    count(distinct product_id) as distinct_product_ids
from joined
group by case when row_seq >= 7 then 'LARGE' else 'SMALL' end, store_id, case when revenue_share >= 0.72 then 'HIGH' else 'LOW' end
order by store_id nulls first;
