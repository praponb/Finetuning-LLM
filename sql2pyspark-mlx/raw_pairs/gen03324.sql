with with_windows as (
    select
        id,
        carrier,
        shipped_date,
        cost,
        status,
        avg(cost) over (partition by carrier order by cost, id rows between 3 preceding and 1 preceding) cost_avg_3_prior,
        sum(cost) over (partition by carrier) carrier_sum_cost,
        lead(cost, 1, 0) over (order by cost, id) next_cost
    from shipments
    where month(shipped_date) in (3, 8, 12)
)
select
    carrier,
    count_if(carrier = 'Kerry') as kerry_events,
    sum(case when carrier = 'DHL' then cost else 0 end) as dhl_cost,
    sort_array(collect_set(status)) as status_list,
    approx_percentile(cost, array(0.25, 0.5, 0.75)) as cost_quartiles,
    count(distinct status) as distinct_status,
    count(*) as row_count,
    grouping(carrier) as grouping_id
from with_windows
group by cube (carrier);
