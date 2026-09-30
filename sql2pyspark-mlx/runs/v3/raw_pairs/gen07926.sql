select id, customer_id, browser from sessions where browser in ('Chrome', 'Safari') union all select id, customer_id, browser from sessions where started_at < '2023-07-31' order by browser;
