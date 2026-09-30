select country, 'supplier' as source from suppliers where rating between 1.8 and 4.0 union all select country, 'session' as source from sessions;
