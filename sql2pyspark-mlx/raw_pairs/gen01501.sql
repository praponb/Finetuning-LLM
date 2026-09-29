select shipped_date from shipments except select opened_date from accounts where status != 'CLOSED';
