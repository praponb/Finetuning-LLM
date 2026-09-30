select * from (select dept_name, location, budget from departments) pivot (avg(budget) for location in ('Chiang Mai', 'New York', 'London', 'Tokyo'));
