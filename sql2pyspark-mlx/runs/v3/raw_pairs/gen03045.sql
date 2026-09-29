with numbered as (select *, row_number() over (partition by browser order by started_at desc, id) rnk from sessions) select id, browser, started_at from numbered where rnk <= 3;
