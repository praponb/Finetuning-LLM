select *, concat(method, ' ', status) as method_status
from payments
order by status;
