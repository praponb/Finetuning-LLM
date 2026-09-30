select status, sum(case when status = 'FROZEN' then 1 else 0 end) frozen_count, count(*) total_count from accounts where opened_date < '2023-09-05' group by status order by status asc;
