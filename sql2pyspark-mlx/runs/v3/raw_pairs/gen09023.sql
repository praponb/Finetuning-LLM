select email from customers union select email from employees where upper(email) like '%LIC%';
