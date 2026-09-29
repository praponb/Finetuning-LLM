select channel, count(*) as row_count from orders where amount >= 1230 group by 1 having sum(case when status = 'SHIPPED' then amount else 0 end) > 39;
