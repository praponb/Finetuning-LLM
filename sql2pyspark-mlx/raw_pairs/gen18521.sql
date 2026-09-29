select email from customers union select email from employees where email like '%@gmail.com' order by email desc;
