from pyspark.sql import functions as F

site_side = (
    df_sites
    .filter((F.col("installed_kw") <= 373.3) & (F.length(F.trim(F.col("site_name"))) >= 7))
    .select("tariff", F.col("commissioned_on").alias("event_date"))
)

site_rows = (
    df_sites
    .filter((F.col("installed_kw") < 386.6) & (~F.col("tariff").isin("TOU", "EV")))
    .select(
        F.col("zone").alias("tariff"),
        F.col("commissioned_on").alias("event_date"),
    )
)

site_side_2 = (
    df_sites
    .filter(F.col("tariff").isin("EV", "FLAT"))
    .select(
        F.col("site_name").alias("tariff"),
        F.col("commissioned_on").alias("event_date"),
    )
)

all_events = (
    site_side
    .union(site_rows)
    .union(site_side_2)
)

result = (
    all_events
    .groupBy("tariff")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("event_date").alias("latest_event_date"),
    )
    .orderBy("tariff")
)
