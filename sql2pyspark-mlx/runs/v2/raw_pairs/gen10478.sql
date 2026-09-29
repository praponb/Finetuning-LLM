select carrier, date_format(delivered_date, 'MMM yyyy') as delivered_date_label
from shipments
where month(delivered_date) in (9, 10, 11) and carrier != 'FedEx'
order by 1 desc;
