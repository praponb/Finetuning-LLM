select *, cast(discount as int) discount_int from orders where amount is not null order by status;
