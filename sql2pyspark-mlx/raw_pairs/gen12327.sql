select id, browser, started_at, customer_id from (select *, row_number() over (partition by browser order by started_at, id) as rnk from sessions) as t where rnk <= 3;
