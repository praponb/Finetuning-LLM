select carrier, max(cost) as max_cost from shipments group by carrier having max(cost) >= 64 order by carrier
