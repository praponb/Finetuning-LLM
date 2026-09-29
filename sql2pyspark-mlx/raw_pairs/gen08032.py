from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy("status_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("status_length")

base = (
    df_payments
    .filter(F.col("paid_at") >= F.to_date(F.lit("2023-06-15")))
    .select("id", "order_id", "status", F.length("status").alias("status_length"))
)

result = (
    base
    .select(
        "id",
        "order_id",
        "status",
        "status_length",
        F.min("status_length").over(w).alias("running_min_status_length"),
        F.avg("status_length").over(w).alias("running_avg_status_length"),
        F.sum("status_length").over(w).alias("running_sum_status_length"),
        F.dense_rank().over(w2).alias("status_length_dense_rank"),
    )
)
