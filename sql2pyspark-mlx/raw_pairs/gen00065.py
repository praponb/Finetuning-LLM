from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("year_start").orderBy(F.col("max_sqft").desc())

kept_keys = (
    df_stores
    .select("city", "id")
    .subtract(
        df_stores
        .filter(F.col("region").isin(["South", "Central"]))
        .select("city", "id")
    )
)

verified = (
    df_stores
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept = (
    verified
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.sale_date") <= F.col("s.opened_date")), "left_anti")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.last_restock") <= F.col("s.opened_date")), "left_anti")
)

remaining_keys = (
    not_flagged
    .select("id")
    .subtract(
        df_stores
        .filter(F.col("region").isNotNull())
        .select("id")
    )
)

reconciled = (
    not_flagged
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

summary = (
    reconciled
    .groupBy(F.date_trunc("year", "opened_date").alias("year_start"), "region")
    .agg(
        F.sum(F.when(F.col("region") == "North", F.col("sqft")).otherwise(0)).alias("north_sqft"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
        F.max("sqft").alias("max_sqft"),
        F.bit_or("sqft").alias("sqft_bit_or"),
    )
    .filter(F.col("sqft_bit_or") < 1000)
    .withColumn("max_sqft_rank", F.rank().over(w))
)

result = (
    summary
    .select(
        "year_start",
        "region",
        "sqft_bit_or",
        F.when(F.col("region").rlike("^(Central|East)$"), "OTHER_ZONE").when(F.col("region").rlike("^(North|South)$"), "PRIORITY").otherwise("CORE").alias("region_zone"),
    )
)
