select
    store_id,
    sum(units) total_units,
    count(*) cnt,
    round(sum(units) * 100.0 / sum(sum(units)) over (), 2) as pct_of_total
from store_sales
where year(sale_date) = 2024
group by store_id
order by total_units desc;
