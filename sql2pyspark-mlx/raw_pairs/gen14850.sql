select year(shipped_date) as shipped_year, sum(cost) as total_cost, sum(sum(cost)) over (order by year(shipped_date)) as cumulative_cost from shipments group by year(shipped_date);
