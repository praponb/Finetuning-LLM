from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w2 = Window.orderBy("sqft_rounded", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("city").orderBy(F.col("inventory_last_restock").desc(), "id")
w4 = Window.partitionBy(F.month("inventory_last_restock"), "region").orderBy("inventory_quantity", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("city").orderBy("inventory_last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("city").orderBy("inventory_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("region").orderBy(F.col("event_seq").desc(), "id")
w8 = Window.partitionBy(F.year("inventory_last_restock"), "city")
w9 = Window.orderBy("running_inventory_quantity", "id").rowsBetween(-2, Window.currentRow)

base = (
    df_stores
    .filter(~F.col("region").isin(["West", "North", "South"]))
    .withColumn("sqft_rounded", F.round("sqft", 0))
)

cleaned = (
    base
    .filter((F.col("opened_date") > F.lit("2023-09-30").cast("date")) & F.col("city").isin(["Berlin", "New York", "Singapore"]))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft_rounded",
        F.lower("store_name").alias("store_name_lower"),
        F.coalesce("region", F.lit("NONE")).alias("resolved_region"),
    )
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

with_windows = (
    latest_match
    .select(
        "id",
        "city",
        "region",
        "sqft_rounded",
        "store_name_lower",
        "resolved_region",
        "inventory_last_restock",
        "inventory_quantity",
        F.avg("sqft_rounded").over(w2).alias("sqft_rounded_avg_5_prior"),
        F.row_number().over(w3).alias("event_seq"),
        F.avg("sqft_rounded").over(w4).alias("sqft_rounded_avg_6_prior"),
        F.sum("sqft_rounded").over(w5).alias("cumulative_sqft_rounded"),
        F.sum("inventory_quantity").over(w6).alias("running_inventory_quantity"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "city",
        "region",
        "store_name_lower",
        "inventory_quantity",
        "event_seq",
        "cumulative_sqft_rounded",
        "running_inventory_quantity",
        F.row_number().over(w7).alias("event_seq_2"),
        F.sum("inventory_quantity").over(w8).alias("inventory_last_restock_year_sum_inventory_quantity"),
        F.avg("cumulative_sqft_rounded").over(w9).alias("cumulative_sqft_rounded_avg_2"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("running_inventory_quantity") >= 1166, "PREMIUM").otherwise("STANDARD").alias("running_inventory_quantity_tier"),
    )
    .agg(
        F.sum(F.when(F.col("region") == "West", F.col("inventory_quantity"))).alias("west_total_inventory_quantity"),
        F.round(F.sum("cumulative_sqft_rounded_avg_2") / F.nullif(F.count(F.when(F.col("city") == "Tokyo", 1)), F.lit(0)), 2).alias("cumulative_sqft_rounded_avg_2_per_tokyo"),
    )
    .orderBy(F.col("cumulative_sqft_rounded_avg_2_per_tokyo").desc())
)
