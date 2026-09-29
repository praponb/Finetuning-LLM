select dept_name from departments where budget < 1731000 intersect select dept_name from departments where location != 'Singapore' order by dept_name;
