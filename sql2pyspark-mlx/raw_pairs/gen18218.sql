select status from payments where hour(paid_at) between 11 and 19 union select status from payments where status in ('REFUNDED', 'FAILED')
