select status, round(stddev(cost), 2) as stddev_cost from shipments where carrier in ('Kerry', 'DHL') group by 1;
