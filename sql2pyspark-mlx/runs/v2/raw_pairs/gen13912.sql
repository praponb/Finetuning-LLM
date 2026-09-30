select
    is_flagged,
    max(amount) - min(amount) amount_range,
    count(distinct amount) unique_merchants,
    sum(case when merchant = '7-Eleven' then amount else 0 end) 7_eleven_amount
from transactions
group by is_flagged
having unique_merchants >= 5;
