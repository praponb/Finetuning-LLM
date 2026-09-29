from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("last_restock")).orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region")
w3 = Window.partitionBy("region").orderBy(F.col("quantity").desc(), "id")
w4 = Window.partitionBy("region").orderBy("quantity_bucket", "id")
w_grp = Window.partitionBy(F.date_trunc("month", "last_restock"), "region").orderBy("latest_rank", "id")
w6 = Window.partitionBy(F.year("last_restock"), "store_id").orderBy("quantity_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_inventory
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "inner")
    .select("e.id", "e.store_id", "e.quantity", "e.last_restock", F.col("d.region"))
)

metrics = (
    enriched
    .filter(F.col("store_id").isin([2, 3, 4, 6]))
    .select(
        "*",
        F.first("region", ignorenulls=True).over(w).alias("first_known_region"),
        F.min("quantity").over(w2).alias("region_min_quantity"),
        F.ntile(5).over(w3).alias("quantity_bucket"),
    )
)

latest = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    latest
    .select(
        "id",
        "quantity",
        "last_restock",
        "region",
        "first_known_region",
        F.lag("region_min_quantity", 2).over(w_grp).alias("prev_region_min_quantity"),
        F.sum("quantity").over(w6).alias("cumulative_quantity"),
    )
)

base = (
    with_windows
    .filter((F.col("last_restock") > F.lit("2023-09-06").cast("date")) & (F.col("cumulative_quantity") == 660))
    .select(
        "id",
        "quantity",
        "last_restock",
        "region",
        "first_known_region",
        F.date_add("last_restock", 74).alias("last_restock_plus_74d"),
        F.coalesce("cumulative_quantity", F.lit(0)).alias("cumulative_quantity_filled"),
    )
)

result = (
    base
    .groupBy(F.date_trunc("MONTH", "last_restock").alias("activity_month"), "region")
    .agg(
        F.round(F.sum("quantity") / F.nullif(F.count(F.when(F.col("region") == "North", 1)), F.lit(0)), 2).alias("quantity_per_north"),
        F.sum(F.when(F.col("region") == "East", F.col("cumulative_quantity_filled")).otherwise(0)).alias("east_cumulative_quantity_filled"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.count(F.when(F.col("region") == "East", 1)).alias("east_rows"),
        F.bit_and("quantity").alias("quantity_bit_and"),
    )
    .filter(F.col("quantity_stddev") < 1000000)
)
