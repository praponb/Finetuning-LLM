from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("cost", "id")
w_recent = Window.partitionBy("order_id", "carrier").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_shipments
    .withColumn("prev_cost", F.lag("cost", 1).over(w))
    .withColumn("last_known_cost", F.last("cost", ignorenulls=True).over(w_recent))
)

result = (
    scored
    .groupBy("carrier")
    .agg(
        F.sum(F.when(F.col("carrier") == "Kerry", F.col("last_known_cost")).otherwise(0)).alias("kerry_last_known_cost"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
        F.avg("cost").alias("avg_cost"),
        F.percentile_approx("prev_cost", 0.95).alias("prev_cost_p95"),
        F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_rows"),
    )
    .filter(F.col("kerry_last_known_cost") > 0)
)
