from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("quantity", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("quantity", "id")
w_ordered = Window.partitionBy("region", "store_id").orderBy("last4_max_quantity", "id").rowsBetween(-2, Window.currentRow)
units_by_store_id = df_store_sales.groupBy("store_id").agg(F.sum("units").alias("sum_store_sale_units"))

filtered = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-03-10").cast("date"))
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.when(F.col("quantity") >= 254, "High").when(F.col("quantity") >= 41, "Medium").otherwise("Low").alias("quantity_tier"),
    )
)

enriched = (
    filtered
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "inner")
    .filter(F.col("d.sqft") <= 14546)
    .select("e.*", F.col("d.city"), F.col("d.region"))
)

metrics = (
    enriched
    .withColumn("last4_max_quantity", F.max("quantity").over(w))
    .withColumn("last4_count_quantity", F.count("quantity").over(w))
    .withColumn("last4_avg_quantity", F.avg("quantity").over(w))
    .withColumn("quantity_bucket", F.ntile(3).over(w2))
)

scored = (
    metrics
    .select(
        "id",
        "store_id",
        "quantity",
        "city",
        "region",
        "last4_max_quantity",
        "last4_avg_quantity",
        F.max("last4_avg_quantity").over(w_ordered).alias("last3_max_last4_avg_quantity"),
        F.min("last4_avg_quantity").over(w_ordered).alias("last3_min_last4_avg_quantity"),
        F.count("last4_avg_quantity").over(w_ordered).alias("last3_count_last4_avg_quantity"),
    )
)

result = (
    scored
    .join(units_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "quantity",
        "region",
        "last4_max_quantity",
        "last3_max_last4_avg_quantity",
        "last3_min_last4_avg_quantity",
        "last3_count_last4_avg_quantity",
        F.col("sum_store_sale_units"),
    )
    .orderBy("last3_max_last4_avg_quantity", F.col("last4_max_quantity").desc())
)
