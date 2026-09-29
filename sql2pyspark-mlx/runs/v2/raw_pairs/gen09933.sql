select id, opened_date, balance from accounts where opened_date < '2024-09-21' and account_type = 'CHECKING' order by balance desc;
