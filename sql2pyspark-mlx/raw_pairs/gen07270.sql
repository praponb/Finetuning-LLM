select status from subscriptions union all select page as status from web_events where page is null;
