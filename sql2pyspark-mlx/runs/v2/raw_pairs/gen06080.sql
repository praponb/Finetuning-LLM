select month(paid_at) as paid_month, count(case when method = 'bank_transfer' then 1 end) as bank_transfer_count from payments group by month(paid_at) having bank_transfer_count > 1
