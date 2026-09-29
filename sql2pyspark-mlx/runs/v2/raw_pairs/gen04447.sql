select id, paid_at, trim(method) clean_method, ceil(amount) amount_whole from payments where status in ('SUCCESS', 'FAILED', 'REFUNDED') and status <> 'FAILED' and status in ('FAILED', 'SUCCESS');
