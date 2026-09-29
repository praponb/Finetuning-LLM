select status from payments except select status from subscriptions where year(end_date) >= 2023;
