select 'order' as kind, status from orders union select 'payment' as kind, status from payments order by kind desc
