select id, channel, discount, rank() over (partition by channel order by discount desc) rnk from orders
