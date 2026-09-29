select status, sum(cost) total_cost, count(*) cnt, round(sum(cost) * 100.0 / sum(sum(cost)) over (), 2) as pct_of_total from shipments group by status order by total_cost desc
