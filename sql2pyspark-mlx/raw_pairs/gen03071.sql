with valid as (
    -- Keep recent, valid rows
    select *, add_months(last_restock, 1) as last_restock_plus_months
    from inventory
    where quantity > 76
)
-- Grouped metrics
select
    date_trunc('quarter', last_restock_plus_months) as quarter_start,
    date_trunc('month', last_restock_plus_months) as activity_month,
    approx_percentile(quantity, 0.95) as quantity_p95,
    percentile_approx(quantity, 0.5) as quantity_p50,
    approx_percentile(quantity, 0.9) as quantity_p90,
    stddev_samp(quantity) as quantity_stddev
from valid
group by date_trunc('quarter', last_restock_plus_months), date_trunc('month', last_restock_plus_months)
order by quantity_p95 desc nulls last
