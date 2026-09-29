select status, order_date from orders where shipping_fee between 29 and 32 intersect select status, order_date from orders where amount >= 590 order by order_date;
