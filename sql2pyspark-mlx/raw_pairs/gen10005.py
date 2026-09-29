from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("last7_sum_quantity", "id")
w4 = Window.partitionBy(F.month("last_restock"), "product_id").orderBy("quantity", "id")
w5 = Window.partitionBy("store_id").orderBy("running_quantity", "id")
w6 = Window.partitionBy("store_id").orderBy("quantity", "id")

scored = (
    df_inventory
    .select(
        "*",
        F.avg("quantity").over(w_part).alias("last7_avg_quantity"),
        F.count("quantity").over(w_part).alias("last7_count_quantity"),
        F.sum("quantity").over(w_part).alias("last7_sum_quantity"),
        F.sum("quantity").over(w2).alias("running_quantity"),
    )
)

scored_inventory = (
    scored
    .filter(F.col("last7_count_quantity") < 8)
    .withColumn("next_last7_sum_quantity", F.lead("last7_sum_quantity", 1, 0).over(w3))
    .withColumn("prev_last7_count_quantity", F.lag("last7_count_quantity", 2).over(w4))
    .withColumn("prev_last7_count_quantity_2", F.lag("last7_count_quantity", 2).over(w5))
)

joined = (
    scored_inventory
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.region"), F.col("p.sqft"))
)

enriched = (
    joined
    .withColumn("next_next_last7_sum_quantity", F.lead("next_last7_sum_quantity").over(w6))
    .withColumn("prev_prev_last7_count_quantity", F.lag("prev_last7_count_quantity").over(w6))
    .withColumn("next_prev_last7_count_quantity", F.lead("prev_last7_count_quantity").over(w6))
    .withColumn("event_seq", F.row_number().over(w6))
)

stats = (
    enriched
    .groupBy(
        F.date_trunc("month", "last_restock").alias("activity_month"),
        "product_id",
    )
    .agg(
        F.round(F.sum("prev_last7_count_quantity") / F.nullif(F.count(F.when(F.col("region") == "North", 1)), F.lit(0)), 2).alias("prev_last7_count_quantity_per_north"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.count(F.when(F.col("region") == "South", 1)).alias("south_rows"),
        F.count(F.when(F.col("region") == "South", 1)).alias("south_count"),
    )
)

result = (
    stats
    .select(
        "activity_month",
        "product_id",
        "quantity_stddev",
        "south_rows",
        "south_count",
    )
)
