select plan, round((100 * sum(case when status = 'PAUSED' then 1 else 0 end)) / count(*), 1) as pct_paused from subscriptions group by plan
