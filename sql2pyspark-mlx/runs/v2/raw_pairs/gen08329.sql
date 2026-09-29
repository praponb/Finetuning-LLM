select status, count(case when carrier = 'FedEx' then 1 end) fedex_count from shipments group by status
