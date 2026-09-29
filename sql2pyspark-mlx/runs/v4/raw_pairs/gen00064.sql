with valid as (
    -- Basic cleansing and defaults
    select *, concat(status, ' - ', channel) as status_channel
    from orders
    where channel is not null
)
-- Deduplicate with ROW_NUMBER
select *, row_number() over (partition by status, store_id order by discount desc nulls last, id) as row_num
from valid
qualify row_num = 1
