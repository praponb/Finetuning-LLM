select case when units >= 38 then 'high' when units >= 11 then 'mid' else 'low' end as units_bucket, sum(units) as total_units, round(sum(units) * 100.0 / sum(sum(units)) over (), 2) as pct_of_total
from store_sales
group by case when units >= 38 then 'high' when units >= 11 then 'mid' else 'low' end;
