select id, browser, started_at, rank() over (partition by browser order by started_at) as rnk from sessions where browser is not null order by id desc
