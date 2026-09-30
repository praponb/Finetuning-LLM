from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc_nulls_last())
w2 = Window.partitionBy("store_id").orderBy("last_restock_month_end", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("last_restock_month_end", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.orderBy("last_restock_month", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy(F.col("first_known_quantity").asc_nulls_last(), "id")
w6 = Window.partitionBy("store_id").orderBy(F.col("quantity_avg_2").asc_nulls_last(), "id")
w7 = Window.partitionBy("store_id")
w8 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("store_id").orderBy("running_quantity", "id")
w10 = Window.partitionBy("store_id").orderBy("running_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_inventory
    .filter(F.col("quantity") == 187)
    .select(
        "id",
        "store_id",
        "quantity",
        F.last_day("last_restock").alias("last_restock_month_end"),
        F.month("last_restock").alias("last_restock_month"),
    )
)

enriched = (
    cleaned
    .withColumn("quantity_cume_dist", F.cume_dist().over(w))
    .withColumn("first_known_quantity", F.first("quantity", ignorenulls=True).over(w2))
    .withColumn("quantity_avg_2", F.avg("quantity").over(w3))
    .withColumn("running_quantity", F.sum("quantity").over(w4))
)

latest = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

latest_inventory = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

scored = (
    latest_inventory
    .withColumn("store_id_sum_quantity_avg_2", F.sum("quantity_avg_2").over(w7))
    .withColumn("first_known_quantity_cume_dist", F.first("quantity_cume_dist", ignorenulls=True).over(w8))
    .withColumn("event_seq", F.row_number().over(w9))
    .withColumn("first_known_quantity_2", F.first("quantity", ignorenulls=True).over(w10))
)

stats = (
    scored
    .groupBy("store_id")
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
        F.bit_or("quantity").alias("quantity_bit_or"),
    )
)

result = (
    stats
    .select("store_id", "quantity_bit_and", "quantity_p95")
    .orderBy(F.col("quantity_p95").desc_nulls_first())
)
