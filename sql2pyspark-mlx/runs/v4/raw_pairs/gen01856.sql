select status, max(cost) - min(cost) as cost_range from shipments group by status having max(cost) - min(cost) >= 27
