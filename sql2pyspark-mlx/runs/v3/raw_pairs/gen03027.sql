select * from accounts where status in ('FROZEN', 'CLOSED') order by balance desc;
