select carrier, date_format(shipped_date, 'yyyy-MM') shipped_month, sum(case when carrier = 'DHL' then 1 else 0 end) dhl_count
from shipments
where order_id is not null and cost > 91
group by carrier, date_format(shipped_date, 'yyyy-MM')
having dhl_count < 30;
