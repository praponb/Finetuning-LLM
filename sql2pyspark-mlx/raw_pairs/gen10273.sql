select 'employee' as record_type, hire_date from employees where hire_date is not null union select 'shipment' as record_type, delivered_date from shipments where delivered_date is not null;
