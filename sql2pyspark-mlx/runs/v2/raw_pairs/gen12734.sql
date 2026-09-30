select *, initcap(trim(status)) as status_title
from accounts
where opened_date between '2023-08-01' and '2023-09-01' and month(opened_date) in (1, 4, 7)
order by account_type desc
