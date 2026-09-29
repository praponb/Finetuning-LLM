select dept_name, location from departments where dept_name = 'Engineering' union select dept_name, location from departments where location = 'Singapore'
