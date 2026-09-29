select * from (select status, count(case when status = 'FAILED' then 1 end) failed_count from payments where order_id in (11, 25, 29, 35) group by status) agg where failed_count >= 9
