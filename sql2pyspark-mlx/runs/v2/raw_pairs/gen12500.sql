select quarter(delivered_date) as delivered_quarter, round(stddev(cost), 2) as stddev_cost from shipments group by 1 having round(stddev(cost), 2) >= 41;
