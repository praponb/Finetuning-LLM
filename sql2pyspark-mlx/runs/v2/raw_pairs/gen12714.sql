select month(shipped_date) shipped_month, count(cost) cost_count from shipments where year(delivered_date) >= 2023 group by month(shipped_date);
