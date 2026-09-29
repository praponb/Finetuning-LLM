with filtered as (
    -- Basic cleansing and defaults
    select *, round(age, 1) age_rounded
    from customers
    where signup_date > date '2023-11-20'
      and city is not null
)
select city, country
from filtered
except all
select city, country
from customers
where length(email) >= 14;
