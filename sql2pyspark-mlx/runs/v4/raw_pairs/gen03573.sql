select email, 'customer' as source from customers union select email, 'employee' as source from employees;
