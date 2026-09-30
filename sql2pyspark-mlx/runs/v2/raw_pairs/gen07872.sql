select shipped_date, status from shipments where month(shipped_date) in (3, 9, 12) except select shipped_date, status from shipments where shipped_date < '2025-03-01';
