with Base as (select * from payments where amount < 570), method_summary as (select method, max(paid_at) as latest_paid_at from Base group by method) select method, latest_paid_at from method_summary
