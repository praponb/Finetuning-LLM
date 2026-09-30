select x.account_type, y.carrier from (select distinct account_type from accounts) x cross join (select distinct carrier from shipments) y order by account_type, carrier;
