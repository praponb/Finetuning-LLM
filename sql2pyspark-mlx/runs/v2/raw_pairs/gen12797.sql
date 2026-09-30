select *
from (
    select
        id,
        status,
        monthly_fee,
        row_number() over (partition by status order by monthly_fee desc, id) as rnk
    from subscriptions
) as r
where rnk <= 3;
