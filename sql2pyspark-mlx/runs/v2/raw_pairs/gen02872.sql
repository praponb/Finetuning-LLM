select date_trunc('MONTH', delivered_date) as delivered_month, status, sum(cost) as total_cost from shipments group by 1, 2 having sum(cost) >= 570 order by delivered_month asc, status asc limit 5;
