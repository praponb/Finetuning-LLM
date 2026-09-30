select month(delivered_date) delivered_month, count(cost) cost_count from shipments group by month(delivered_date) order by cost_count desc, delivered_month;
