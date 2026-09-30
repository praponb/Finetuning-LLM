select *, coalesce(country, 'unassigned') country_filled, length(device) device_length
from sessions
where country = 'TH';
