select *, row_number() over (order by shipping_fee desc, id) as row_seq, ntile(10) over (partition by status order by discount desc, id) as discount_bucket from orders where amount > 570;
