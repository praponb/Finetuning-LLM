select year(shipped_date) as shipped_year, sum(case when carrier = 'Kerry' then cost else 0 end) as kerry_cost, avg(cost) as avg_cost from shipments group by year(shipped_date) order by avg_cost;
