from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy(F.col("quantity").asc_nulls_first())
w3 = Window.partitionBy("product_id", "store_id").orderBy(F.col("last7_avg_quantity").desc(), "id")
amount_by_store_id = df_orders.groupBy("store_id").agg(F.min("amount").alias("min_order_amount"))

metrics = (
    df_inventory
    .withColumn("last7_sum_quantity", F.sum("quantity").over(w))
    .withColumn("last7_avg_quantity", F.avg("quantity").over(w))
    .withColumn("last7_count_quantity", F.count("quantity").over(w))
    .withColumn("last7_min_quantity", F.min("quantity").over(w))
    .withColumn("quantity_cume_dist", F.cume_dist().over(w2))
)

ranked = (
    metrics
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

enriched = (
    ranked
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.sqft").isNotNull())
    .select("a.*", F.col("s.sqft"), F.col("s.city"))
)

joined = (
    enriched
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.region"), F.col("b.store_name"))
)

result = (
    joined
    .join(amount_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "last7_sum_quantity",
        "last7_count_quantity",
        "rn",
        "sqft",
        F.col("min_order_amount"),
    )
    .orderBy(F.col("min_order_amount").desc_nulls_last())
)
