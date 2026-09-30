select status, 'order' record_type from orders union select status, 'account' record_type from accounts order by status;
