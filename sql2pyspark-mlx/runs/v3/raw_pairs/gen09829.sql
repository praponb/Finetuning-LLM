select id, paid_at from payments where (method <> 'promptpay' and amount >= 880) or method = 'bank_transfer' order by paid_at asc, id desc limit 20
