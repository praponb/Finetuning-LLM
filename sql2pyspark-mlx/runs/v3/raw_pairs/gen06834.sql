select *, upper(method) as method_upper, cast(paid_at as date) as paid_at_day
from payments;
