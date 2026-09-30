select
    id,
    amount,
    is_flagged,
    cast(is_flagged as int) is_flagged_flag
from transactions
where amount between 3700 and 3800;
