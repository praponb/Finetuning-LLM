select id from customers intersect all select id from customers where segment in ('Consumer', 'SMB')
