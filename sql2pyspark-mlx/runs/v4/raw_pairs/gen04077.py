from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("last_restock")

cleaned = (
    df_inventory
    .filter(F.col("store_id").isin([3, 4]))
    .withColumn("quantity_str", F.col("quantity").cast("double"))
)

deduped = (
    cleaned
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

valid = (
    deduped
    .filter(F.col("last_restock") >= F.lit("2023-10-04").cast("date"))
    .withColumn("last_restock_dayofweek", F.dayofweek("last_restock"))
)

summary = (
    valid
    .groupBy(
        F.when(F.col("quantity_str") >= 68, "LARGE").otherwise("SMALL").alias("quantity_str_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.round(F.avg("last_restock_dayofweek"), 2).alias("avg_last_restock_dayofweek"),
        F.percentile_approx("quantity_str", 0.5).alias("quantity_str_p50"),
        F.percentile_approx("quantity_str", 0.9).alias("quantity_str_p90"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
    )
)

result = (
    summary
    .select(
        "quantity_str_tier",
        "event_count",
        "avg_last_restock_dayofweek",
        "quantity_str_p50",
        "quantity_str_p90",
        "quantity_quartiles",
        "distinct_product_ids",
    )
    .orderBy(F.col("quantity_str_p50").asc_nulls_first())
)
