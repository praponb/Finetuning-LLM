select status from accounts where year(opened_date) = 2023 except select status from subscriptions order by status
