from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock_period", "id")
w2 = Window.partitionBy("store_id").orderBy("last_restock_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc())
w4 = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id")
w6 = Window.orderBy("quantity", "id").rowsBetween(-3, -1)
w7 = Window.partitionBy("product_id").orderBy("quantity_bucket", "id")

base = (
    df_inventory
    .filter(F.col("last_restock") > F.to_date(F.lit("2024-03-15")))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.date_trunc("MONTH", "last_restock").alias("last_restock_period"),
    )
)

with_gaps = (
    base
    .withColumn("is_new_session", F.when(F.lag("last_restock_period").over(w).isNull() | (F.col("last_restock_period") > F.lag("last_restock_period").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

latest = (
    sessionized
    .withColumn("row_num", F.rank().over(w3))
    .filter(F.col("row_num") == 1)
)

scored = (
    latest
    .withColumn("running_session_no", F.sum("session_no").over(w4))
    .withColumn("quantity_bucket", F.ntile(3).over(w5))
)

valid = (
    scored
    .filter(F.col("session_no") >= 4)
    .select(
        "*",
        F.date_trunc("MONTH", "last_restock_period").alias("last_restock_period_period"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.avg("quantity").over(w6).alias("quantity_avg_3_prior"),
        F.ntile(4).over(w7).alias("quantity_bucket_bucket"),
    )
)

result = (
    with_windows
    .groupBy("store_id")
    .agg(
        F.percentile_approx("is_new_session", 0.95).alias("is_new_session_p95"),
        F.avg("quantity").alias("avg_quantity"),
        F.max("last_restock_period").alias("last_seen"),
        F.round(F.avg("quantity_avg_3_prior"), 2).alias("avg_quantity_avg_3_prior"),
        F.sum("session_no").alias("total_session_no"),
        F.min("running_session_no").alias("min_running_session_no"),
    )
    .filter(F.col("total_session_no") > 0)
)
