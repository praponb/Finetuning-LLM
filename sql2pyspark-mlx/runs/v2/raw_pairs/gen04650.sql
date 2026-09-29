select
    month(opened_date) as opened_month,
    sum(sqft) as total_sqft,
    count(*) as cnt,
    round(sum(sqft) * 100.0 / sum(sum(sqft)) over (), 2) as pct_of_total
from stores
where region not in ('East', 'Central', 'West')
group by month(opened_date)
order by cnt desc;
