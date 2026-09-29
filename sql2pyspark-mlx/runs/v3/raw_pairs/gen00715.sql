select dept_name, length(location) as location_length from departments where budget > 1926000 and (budget <= 2276000 or dept_name in ('Legal', 'Engineering', 'Finance'))
