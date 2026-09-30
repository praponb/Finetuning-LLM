select country, 'customer' as kind from customers union all select browser, 'session' as kind from sessions where country is not null;
