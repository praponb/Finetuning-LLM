with cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, datediff(current_date(), sale_date) / 7 as weeks_since_sale_date
    from store_sales
    where units between 23 and 40
),
matched_keys as (
    -- Set intersection on keys
    select id
    from cleaned
    intersect
    select id
    from store_sales
    where revenue < 1900
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    inner join matched_keys k on s.id = k.id
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        percent_rank() over (partition by store_id, product_id order by weeks_since_sale_date desc nulls last) as weeks_since_sale_date_percent_rank,
        cume_dist() over (partition by store_id order by revenue desc) as revenue_cume_dist,
        corr(units, revenue) over (partition by store_id, product_id) as corr_units_revenue,
        round(units / sum(units) over (partition by date_trunc('month', sale_date), product_id), 4) as units_share
    from kept_rows
),
joined as (
    -- Attach stores attributes
    select
        s.*,
        p.city,
        p.store_name,
        p.region
    from enriched s
    left join stores p
        on s.store_id = p.id
)
-- Native SQL UNPIVOT (wide to long)
select
    store_id,
    measure,
    metric_value
from (
    select
        store_id,
        corr_units_revenue,
        units_share,
        weeks_since_sale_date
    from joined
) src
unpivot include nulls (
    metric_value for measure in (corr_units_revenue, units_share, weeks_since_sale_date)
);
