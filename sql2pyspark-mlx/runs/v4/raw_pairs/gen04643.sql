select shipped_date from shipments where month(shipped_date) in (3, 4, 7) except select hire_date as shipped_date from employees where manager_id in (17, 21, 25) order by shipped_date desc
