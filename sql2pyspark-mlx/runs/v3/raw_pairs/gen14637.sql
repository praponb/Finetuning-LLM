select *
from (
    select is_flagged, case when merchant in ('Lazada', 'Grab') then 'tier_1' else 'other' end as merchant_group, round(avg(amount), 2) as avg_amount
    from transactions
    group by is_flagged, case when merchant in ('Lazada', 'Grab') then 'tier_1' else 'other' end
) as agg
where avg_amount >= 63
order by merchant_group
