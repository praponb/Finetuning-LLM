select max(end_date) as last_end_date, count(case when status = 'CANCELLED' then 1 end) as cancelled_count from subscriptions order by cancelled_count desc
