from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.month("opened_date"), "region").orderBy(F.col("quantity_filled").desc(), "id")
w4 = Window.partitionBy("region", "store_id").orderBy(F.col("sqft").desc(), "id")
w5 = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(-2, Window.currentRow)
w_grp = Window.partitionBy("region").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_inventory
    .filter((F.col("last_restock") > F.lit("2023-08-20").cast("date")) & (F.col("quantity") == 201))
    .select(
        "*",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
        F.quarter("last_restock").alias("last_restock_quarter"),
    )
)

valid = (
    cleaned
    .filter(F.col("last_restock") >= F.lit("2023-06-30").cast("date"))
    .withColumn("quantity_str", F.col("quantity").cast("string"))
    .withColumn("quantity_minus_quantity_filled", (F.col("quantity") - F.col("quantity_filled")))
)

deduped = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        "quantity_filled",
        "quantity_str",
        "quantity_minus_quantity_filled",
    )
)

enriched = (
    deduped
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "inner")
    .select(
        "s.id",
        "s.store_id",
        "s.last_restock",
        "s.quantity_filled",
        F.col("d.region"),
        F.col("d.opened_date"),
        F.col("d.sqft"),
    )
)

scored = (
    enriched
    .filter(F.col("region").isin(["East", "Central", "South"]))
    .select(
        "id",
        "last_restock",
        "region",
        "opened_date",
        "sqft",
        F.sum("quantity_filled").over(w2).alias("cumulative_quantity_filled"),
        F.row_number().over(w3).alias("row_seq"),
        F.row_number().over(w4).alias("event_seq"),
        F.avg("quantity_filled").over(w5).alias("quantity_filled_avg_2"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "last_restock",
        "region",
        "sqft",
        "cumulative_quantity_filled",
        "row_seq",
        F.max("sqft").over(w_grp).alias("running_max_sqft"),
        F.sum("sqft").over(w_grp).alias("running_sum_sqft"),
        F.min("sqft").over(w_grp).alias("running_min_sqft"),
    )
)

filtered = (
    metrics
    .filter((F.col("last_restock") >= F.to_date(F.lit("2023-09-06"))) & F.col("region").isNotNull())
    .withColumn("running_sum_sqft_minus_running_max_sqft", (F.col("running_sum_sqft") - F.col("running_max_sqft")))
    .withColumn("region_clean", F.coalesce("region", F.lit("DIRECT")))
)

result = (
    filtered
    .select(
        "id",
        "region",
        "cumulative_quantity_filled",
        "running_max_sqft",
        "running_sum_sqft",
        "running_sum_sqft_minus_running_max_sqft",
        "region_clean",
    )
    .orderBy(F.col("running_sum_sqft").asc_nulls_last())
)
