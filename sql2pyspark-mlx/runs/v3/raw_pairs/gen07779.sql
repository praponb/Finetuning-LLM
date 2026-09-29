SELECT id, cost, PERCENT_RANK() OVER (ORDER BY cost DESC) cost_pct_rank FROM shipments;
