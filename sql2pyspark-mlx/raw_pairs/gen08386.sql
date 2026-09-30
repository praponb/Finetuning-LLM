select is_flagged, merchant, round((100 * sum(case when merchant = 'Starbucks' then 1 else 0 end)) / count(*), 1) as pct_starbucks
from transactions
group by is_flagged, merchant
order by pct_starbucks, merchant;
