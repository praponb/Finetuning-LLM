select month(delivered_date) as delivered_month, status, max(cost) as max_cost, round(avg(cost), 2) as avg_cost from shipments group by month(delivered_date), status
