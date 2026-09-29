from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")
w2 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy(F.date_trunc("MONTH", "last_restock"), "store_id")
w4 = Window.partitionBy(F.month("last_restock"), "store_id").orderBy("cumulative_quantity", "id")
w5 = Window.partitionBy("store_region").orderBy("store_sqft", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("store_id")
w7 = Window.partitionBy("store_region", "store_id").orderBy("store_sqft", "id")
w8 = Window.partitionBy("store_id").orderBy(F.col("store_sqft").desc())
units_by_store_id = df_store_sales.groupBy("store_id").agg(F.min("units").alias("min_store_sale_units"))

enriched = (
    df_inventory
    .filter(F.col("quantity").between(73, 158))
    .select(
        "id",
        "store_id",
        "last_restock",
        F.row_number().over(w).alias("event_seq"),
        F.sum("quantity").over(w2).alias("cumulative_quantity"),
    )
)

with_dims = (
    enriched
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .select("e.*", F.col("s.city"))
)

scored = (
    with_dims
    .select(
        "id",
        "store_id",
        "event_seq",
        "cumulative_quantity",
        "city",
        F.count("cumulative_quantity").over(w_recent).alias("last_restock_period_count_cumulative_quantity"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

joined = (
    scored
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.store_id",
        "a.seq_no",
        F.col("p.sqft"),
        F.col("p.store_name"),
        F.col("p.region"),
    )
)

enriched_inventory = (
    joined
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.store_name",
        F.col("p.region").alias("store_region"),
        F.col("p.sqft").alias("store_sqft"),
    )
)

metrics = (
    enriched_inventory
    .withColumn("store_sqft_avg_6", F.avg("store_sqft").over(w5))
    .withColumn("store_sqft_share", F.round((F.col("store_sqft") / F.sum("store_sqft").over(w6)), 4))
    .withColumn("store_sqft_share_2", F.round((F.col("store_sqft") / F.sum("store_sqft").over(w6)), 4))
    .withColumn("store_sqft_bucket", F.ntile(5).over(w7))
)

latest = (
    metrics
    .withColumn("latest_rank", F.rank().over(w8))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "store_id",
        "store_region",
        "store_sqft",
        "store_sqft_avg_6",
        "store_sqft_share",
        "store_sqft_share_2",
    )
)

result = (
    latest
    .join(units_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "store_sqft",
        "store_sqft_avg_6",
        "store_sqft_share",
        "store_sqft_share_2",
        F.col("min_store_sale_units"),
    )
    .orderBy("store_id")
)
