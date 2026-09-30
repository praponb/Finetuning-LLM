select id, status, amount, row_number() over (partition by status order by amount, id) as row_num from payments where status in ('SUCCESS', 'REFUNDED')
