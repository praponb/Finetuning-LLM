select id, status, opened_date from (select id, status, opened_date, rank() over (partition by status order by opened_date desc) rn from accounts) t where rn = 1 order by 2;
