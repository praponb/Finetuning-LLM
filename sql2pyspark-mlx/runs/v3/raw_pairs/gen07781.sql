select cost as value from shipments intersect select amount as value from transactions where merchant is null;
