with top_sessions as (select *, row_number() over (partition by browser order by started_at desc, id) as rnk from sessions) select id, browser, started_at, country from top_sessions where rnk <= 3;
