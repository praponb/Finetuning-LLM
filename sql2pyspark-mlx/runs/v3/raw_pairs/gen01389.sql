select id, status, date_trunc('YEAR', delivered_date) as delivered_date_period
from shipments;
