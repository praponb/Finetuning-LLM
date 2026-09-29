select id, status, opened_date, customer_id from (select *, rank() over (partition by status order by opened_date) as rn from accounts) as r where rn <= 2;
