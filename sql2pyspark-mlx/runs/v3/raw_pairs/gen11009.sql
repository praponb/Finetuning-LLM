select started_at, datediff(current_date(), started_at) days_since_started_at from sessions where device != 'tablet' and country in ('DE', 'US', 'SG')
