select id, balance, date_sub(opened_date, 7) as opened_date_minus_7d
from accounts;
