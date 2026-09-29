with enriched as (
    -- Ranking and gap features
    select
        *,
        avg(units) over (partition by store_id order by revenue, id rows between 5 preceding and 1 preceding) as units_avg_5_prior,
        ntile(3) over w_part as revenue_bucket,
        avg(revenue) over (partition by store_id order by revenue, id rows between 3 preceding and 1 preceding) as revenue_avg_3_prior
    from store_sales
    window w_part as (order by revenue, id)
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        last_value(revenue_avg_3_prior) ignore nulls over (order by revenue_avg_3_prior, id rows between unbounded preceding and current row) as last_known_revenue_avg_3_prior,
        first_value(revenue_bucket) ignore nulls over w_grp as first_known_revenue_bucket,
        rank() over (partition by product_id order by units desc) as units_rank
    from enriched
    window w_grp as (order by revenue_bucket, id rows between unbounded preceding and current row)
),
with_dims as (
    -- Attach products attributes
    select
        a.*,
        p.launch_date,
        p.product_name
    from scored a
    left join products p
        on a.product_id = p.id
)
-- Grouped metrics
select
    case when revenue_bucket >= 7 then 'LARGE' else 'SMALL' end as revenue_bucket_tier,
    count(distinct product_id) as distinct_product_ids,
    bit_and(units) as units_bit_and
from with_dims
group by 1
