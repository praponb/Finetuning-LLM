select country, min_credit_limit, country_values
from (
    select country, min(credit_limit) as min_credit_limit, concat_ws(', ', sort_array(collect_set(country))) as country_values
    from customers
    group by country
) as t
where min_credit_limit >= 16000;
