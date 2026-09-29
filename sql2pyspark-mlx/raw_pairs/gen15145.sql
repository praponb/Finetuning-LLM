select dept_name from departments where location in ('Chiang Mai', 'Tokyo', 'Berlin') union all select dept_name from departments where location is not null
