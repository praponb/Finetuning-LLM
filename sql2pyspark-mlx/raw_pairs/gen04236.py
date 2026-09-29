from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy("quantity", "id")

valid = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-09-20").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        F.coalesce("region", F.lit("unassigned")).alias("region_filled"),
        F.substring("city", 1, 4).alias("city_prefix"),
    )
)

with_latest = (
    valid
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.region",
        "s.region_filled",
        "s.city_prefix",
        F.col("c.last_restock"),
        F.col("c.quantity"),
    )
)

kept = (
    with_latest
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.last_restock") < F.col("s.last_restock")), "left_anti")
)

deduped = (
    kept
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

stats = (
    deduped
    .withColumn("year_start", F.date_trunc("year", "last_restock"))
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 189, "PREMIUM").otherwise("STANDARD"))
    .rollup("year_start", "quantity_tier")
    .agg(
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.round(F.sum("quantity") / F.nullif(F.count(F.when(F.col("city") == "Tokyo", 1)), F.lit(0)), 2).alias("quantity_per_tokyo"),
        F.count("*").alias("cnt"),
        F.countDistinct("city").alias("distinct_cities"),
        F.sum(F.when(F.col("region") == "West", F.col("quantity")).otherwise(0)).alias("west_quantity"),
        F.round(F.sum("quantity") / F.nullif(F.count(F.when(F.col("city") == "Bangkok", 1)), F.lit(0)), 2).alias("quantity_per_bangkok"),
        F.grouping("year_start").alias("grouping_level"),
    )
)

result = (
    stats
    .select(
        "year_start",
        "quantity_tier",
        "quantity_quartiles",
        "quantity_per_tokyo",
        "cnt",
        "west_quantity",
        "quantity_per_bangkok",
    )
    .orderBy(F.col("quantity_tier").desc())
)
