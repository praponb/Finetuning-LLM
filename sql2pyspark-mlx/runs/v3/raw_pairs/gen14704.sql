select * from (select *, row_number() over (partition by status order by monthly_fee desc, id) as pos from subscriptions) as t where pos <= 3 order by end_date
