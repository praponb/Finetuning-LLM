from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy("sqft_filled", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("days_inventory_last_restock_to_opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("first_known_sqft_filled", "id")
w6 = Window.partitionBy("region").orderBy(F.col("days_inventory_last_restock_to_opened_date").desc(), "id")
w7 = Window.partitionBy("region").orderBy(F.col("last_restock").desc_nulls_last(), "id")

cleaned = (
    df_stores
    .filter(~F.col("city").isin(["Chiang Mai", "London"]))
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        F.coalesce("sqft", F.lit(0)).alias("sqft_filled"),
    )
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock").alias("inventory_last_restock"))
)

with_windows = (
    with_latest
    .select(
        "*",
        F.avg("sqft_filled").over(w2).alias("sqft_filled_avg_5_prior"),
        F.first("sqft_filled", ignorenulls=True).over(w3).alias("first_known_sqft_filled"),
    )
)

with_last = (
    with_windows
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.last_restock"),
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

valid = (
    with_last
    .filter(F.month("inventory_last_restock").isin([3, 4, 7]))
    .select(
        "id",
        "city",
        "region",
        "first_known_sqft_filled",
        "last_restock",
        "inventory_quantity",
        F.floor("first_known_sqft_filled").alias("first_known_sqft_filled_whole"),
        F.datediff("opened_date", "inventory_last_restock").alias("days_inventory_last_restock_to_opened_date"),
    )
)

with_windows_stores = (
    valid
    .select(
        "*",
        F.sum("first_known_sqft_filled_whole").over(w4).alias("running_first_known_sqft_filled_whole"),
        F.ntile(10).over(w5).alias("first_known_sqft_filled_bucket"),
        F.ntile(5).over(w6).alias("days_inventory_last_restock_to_opened_date_bucket"),
    )
)

ranked = (
    with_windows_stores
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

daily = (
    ranked
    .groupBy(
        F.when(F.col("first_known_sqft_filled_bucket") >= 9, "HIGH").otherwise("LOW").alias("first_known_sqft_filled_bucket_tier"),
        "city",
        F.when(F.col("running_first_known_sqft_filled_whole") >= 354, "HIGH").otherwise("LOW").alias("running_first_known_sqft_filled_whole_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("days_inventory_last_restock_to_opened_date_bucket", [0.25, 0.5, 0.75]).alias("days_inventory_last_restock_to_opened_date_bucket_quartiles"),
        F.round(F.avg("first_known_sqft_filled"), 2).alias("avg_first_known_sqft_filled"),
    )
)

result = (
    daily
    .select(
        "first_known_sqft_filled_bucket_tier",
        "city",
        "running_first_known_sqft_filled_whole_tier",
    )
    .orderBy("running_first_known_sqft_filled_whole_tier")
)
