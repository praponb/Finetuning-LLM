SELECT a.channel, b.status FROM (SELECT DISTINCT channel FROM orders) a CROSS JOIN (SELECT DISTINCT status FROM payments) b;
