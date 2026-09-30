select channel, sum(shipping_fee) as total_shipping_fee, dense_rank() over (order by sum(shipping_fee) desc) as rnk from orders where amount < 460 group by channel order by channel;
