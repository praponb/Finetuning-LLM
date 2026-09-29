from pyspark.sql import functions as F

base = (
    df_stores
    .filter(F.col("opened_date") > F.to_date(F.lit("2023-07-18")))
    .withColumn("years_since_opened_date", (F.datediff(F.current_date(), "opened_date") / 365))
)

remaining_keys = (
    base
    .select("city")
    .subtract(
        df_stores
        .filter(F.col("opened_date") > "2024-08-25")
        .select("city")
    )
)

reconciled = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("region").isNotNull())
    .select(
        "*",
        (F.col("years_since_opened_date") + F.col("sqft")).alias("years_since_opened_date_plus_sqft"),
        F.initcap(F.trim("city")).alias("city_title"),
    )
)

eligible = (
    valid
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") <= "2025-04-01"), "left_semi")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.order_date") >= F.col("s.opened_date")), "left_semi")
)

result = (
    not_flagged
    .select(
        "store_name",
        "region",
        "opened_date",
        "years_since_opened_date",
        "years_since_opened_date_plus_sqft",
        F.when(F.col("region").rlike("^(South|West)$"), "PRIORITY").otherwise("CORE").alias("region_zone"),
    )
    .orderBy(
        F.col("years_since_opened_date").asc_nulls_last(),
        F.col("region_zone").desc(),
    )
)
