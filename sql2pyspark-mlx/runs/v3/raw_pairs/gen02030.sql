select case when cost >= 87 then 'high' when cost >= 40 then 'mid' else 'low' end as cost_bucket, max(shipped_date) as latest_shipped_date
from shipments
group by case when cost >= 87 then 'high' when cost >= 40 then 'mid' else 'low' end;
