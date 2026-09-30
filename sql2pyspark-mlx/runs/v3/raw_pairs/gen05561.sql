select shipped_date, 'shipment' as source from shipments union all select hire_date, 'employee' as source from employees where manager_id in (15, 28, 36, 40) order by shipped_date desc
