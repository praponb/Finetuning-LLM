select order_id, max(shipped_date) as latest_shipped_date, count(case when carrier = 'FedEx' then 1 end) as fedex_count from shipments where shipped_date < '2023-09-01' group by 1 order by order_id;
