select is_flagged, min(amount) as min_amount from transactions where txn_type <> 'REFUND' and txn_type = 'FEE' group by is_flagged
