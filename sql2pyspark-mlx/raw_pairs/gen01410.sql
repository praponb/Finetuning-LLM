select city from suppliers where rating <= 4.1 union all select city from stores where city in ('Tokyo', 'Chiang Mai', 'Singapore');
