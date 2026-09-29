select id, method, status, length(status) as status_length from payments where hour(paid_at) between 11 and 14 and (hour(paid_at) between 7 and 11 or amount >= 1300)
