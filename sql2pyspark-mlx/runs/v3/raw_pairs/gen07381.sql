select
    delivered_month,
    carrier,
    status_list,
    max_cost
from (
    select
        date_format(delivered_date, 'yyyy-MM') as delivered_month,
        carrier,
        sort_array(collect_set(status)) as status_list,
        max(cost) as max_cost
    from shipments
    where delivered_date >= '2024-12-01'
    group by date_format(delivered_date, 'yyyy-MM'), carrier
) as t
where max_cost < 30
order by 4 desc;
