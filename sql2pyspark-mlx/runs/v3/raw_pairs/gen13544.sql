select plan from subscriptions where year(end_date) >= 2024 except select plan from subscriptions where monthly_fee > 160;
