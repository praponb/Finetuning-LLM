select status, coalesce(carrier, 'N/A') carrier_filled
from shipments
where year(shipped_date) >= 2025 or status in ('IN_TRANSIT', 'DELIVERED');
