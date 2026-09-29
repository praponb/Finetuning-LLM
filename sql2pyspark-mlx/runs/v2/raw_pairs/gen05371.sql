select status, sum(cost) as total_cost, count(case when carrier = 'Flash' then 1 end) as flash_count from shipments where order_id in (19, 26, 31, 36) group by status;
