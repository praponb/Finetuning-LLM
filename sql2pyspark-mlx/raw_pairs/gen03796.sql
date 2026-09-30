with deduped as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by status order by monthly_fee desc, id asc) as rn
    from subscriptions
    qualify rn = 1
)
select
    id,
    plan,
    end_date,
    monthly_fee,
    status,
    rn,
    case when status rlike '^(CANCELLED|PAUSED)$' then 'GROUP_A' when status rlike '^(ACTIVE|EXPIRED)$' then 'OTHER_ZONE' else 'PRIORITY' end as status_zone
from deduped
order by status_zone asc nulls last, end_date asc;
