select country, segment_group, credit_limit_count
from (
    select country, case when segment in ('SMB', 'Consumer') then 'tier_1' else 'other' end as segment_group, count(credit_limit) as credit_limit_count
    from customers
    group by country, case when segment in ('SMB', 'Consumer') then 'tier_1' else 'other' end
) as agg
where credit_limit_count >= 5
order by segment_group desc;
