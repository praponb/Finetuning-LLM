select *, length(country) country_length from customers where city <> 'Berlin' order by city, email;
