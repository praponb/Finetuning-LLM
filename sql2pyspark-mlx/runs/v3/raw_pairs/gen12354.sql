select id, is_flagged, substring(merchant, 1, 2) as merchant_prefix
from transactions
where not is_flagged and account_id in (23, 25, 28);
