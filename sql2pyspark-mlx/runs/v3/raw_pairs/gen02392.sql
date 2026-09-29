with top_accounts as (select id, status, balance, row_number() over (partition by status order by balance desc, id) as rnk from accounts) select id, status, balance from top_accounts where rnk <= 3
