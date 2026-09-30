with ranked as (
    select
        id,
        status,
        monthly_fee,
        row_number() over (partition by status order by monthly_fee, id) as rnk
    from subscriptions
)
select id, status, monthly_fee
from ranked
where rnk = 1;
