select *, last_day(delivered_date) as delivered_date_month_end from shipments where carrier not in ('Kerry', 'FedEx', 'Flash') or order_id is null order by status
