select dept_name from departments where location in ('Chiang Mai', 'Tokyo') intersect select dept_name from departments where location in ('Tokyo', 'Berlin') order by dept_name desc
