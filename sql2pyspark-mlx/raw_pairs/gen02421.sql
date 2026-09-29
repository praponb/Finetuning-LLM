select
    location,
    sum(budget) as total_budget,
    count(*) as cnt,
    round(sum(budget) * 100.0 / sum(sum(budget)) over (), 2) as pct_of_total
from departments
where budget < 1925000
group by location;
