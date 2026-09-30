select date_trunc('MONTH', delivered_date) delivered_month, avg(cost) avg_cost, max(cost) - min(cost) cost_range from shipments group by 1 order by 2;
