select 'employee' as origin, email from employees union all select 'payment' as origin, method from payments where method not in ('card', 'promptpay');
