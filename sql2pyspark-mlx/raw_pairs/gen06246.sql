select id, order_id, cost, round(cost / sum(cost) over (partition by order_id), 3) as cost_share, round(cost / sum(cost) over (partition by order_id), 3) as cost_share_2 from shipments;
