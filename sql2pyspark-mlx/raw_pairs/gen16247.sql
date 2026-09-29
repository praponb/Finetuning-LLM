select status from orders where shipping_fee is null except select status from subscriptions order by status;
