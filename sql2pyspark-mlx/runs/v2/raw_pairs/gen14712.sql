select status from orders where channel in ('mobile', 'store') intersect select status from accounts where account_type = 'LOAN' order by status
