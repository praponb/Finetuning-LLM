select id, country from customers where country not in ('UK', 'JP') union all select id, country from customers where credit_limit > 15600
