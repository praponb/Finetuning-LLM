select status, method, min(paid_at) as min_paid_at, sum(case when status = 'FAILED' then 1 else 0 end) as failed_count from payments group by status, method;
