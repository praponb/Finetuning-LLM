select location, sum(budget) as total_budget, round(sum(budget) * 100.0 / sum(sum(budget)) over (), 2) as pct_of_total from departments where location in ('Chiang Mai', 'Berlin') group by location
