select signup_quarter, signup_year, unique_segments
from (
    select quarter(signup_date) as signup_quarter, year(signup_date) as signup_year, count(distinct credit_limit) as unique_segments
    from customers
    group by quarter(signup_date), year(signup_date)
) as agg
where unique_segments >= 5;
