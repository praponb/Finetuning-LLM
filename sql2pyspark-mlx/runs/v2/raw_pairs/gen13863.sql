select email, 'customer' as source from customers where age between 50 and 53 union all select email, 'employee' as source from employees order by source
