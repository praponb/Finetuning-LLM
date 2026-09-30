select method, status, date_format(paid_at, 'yyyy-MM-dd') as paid_at_label
from payments;
