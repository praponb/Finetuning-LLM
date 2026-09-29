SELECT dept_name FROM departments EXCEPT SELECT supplier_name FROM suppliers WHERE country = 'TH' ORDER BY dept_name;
