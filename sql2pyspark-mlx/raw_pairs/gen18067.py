from pyspark.sql import functions as F

cleaned = (
    df_stores
    .filter(~F.col("city").isin(["Singapore", "New York", "Berlin"]) & (F.col("opened_date") >= F.to_date(F.lit("2023-10-07"))))
    .withColumn("city_title", F.initcap(F.trim("city")))
    .withColumn("days_since_opened_date", F.datediff(F.current_date(), "opened_date"))
    .withColumn("resolved_region", F.coalesce("region", F.lit("NONE")))
)

kept_keys = (
    cleaned
    .select("id", "city")
    .exceptAll(
        df_stores
        .filter(F.col("opened_date") > "2025-07-06")
        .select("id", "city")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

not_flagged = (
    verified
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.units") != 11), "left_semi")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.order_date") < F.col("s.opened_date")), "left_semi")
)

kept = (
    eligible
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.sale_date") <= F.col("s.opened_date")), "left_anti")
)

result = (
    kept
    .select(
        "store_name",
        "region",
        "opened_date",
        "sqft",
        "days_since_opened_date",
        "resolved_region",
    )
)
