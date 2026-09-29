select txn_type, round((100 * sum(case when merchant = 'Grab' then 1 else 0 end)) / count(*), 1) as pct_grab
from transactions
group by 1
order by pct_grab asc;
