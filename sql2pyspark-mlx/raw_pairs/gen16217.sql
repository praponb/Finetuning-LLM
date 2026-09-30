with with_windows as (
    select
        *,
        lag(units) over w_recent as prev_units,
        lead(units) over w_recent as next_units,
        lag(revenue) over w_recent as prev_revenue,
        lead(revenue) over w_recent as next_revenue,
        row_number() over w_recent as event_seq
    from store_sales
    where units >= 26
    window w_recent as (partition by store_id, product_id order by units, id)
)
select
    date_trunc('week', sale_date) as week_start,
    product_id,
    count(*) as cnt,
    percentile_approx(prev_units, 0.95) as prev_units_p95,
    corr(revenue, units) as corr_revenue_units
from with_windows
group by 1, 2
having prev_units_p95 > 0
order by cnt desc;
