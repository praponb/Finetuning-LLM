select *, concat(name, ' - ', email) as name_email from customers where country in ('US', 'UK', 'SG') and segment = 'Consumer';
