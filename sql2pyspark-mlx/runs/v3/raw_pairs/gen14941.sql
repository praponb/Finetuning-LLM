select id, device from sessions where browser is not null union select id, device from sessions where device in ('tablet', 'desktop');
