from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy(F.year("paid_at"), "method")

cleaned = (
    df_payments
    .filter(F.col("method").isNotNull() & (~F.col("method").eqNullSafe(F.lit("promptpay"))))
    .withColumn("amount_int", F.col("amount").cast("int"))
    .withColumn("resolved_method", F.coalesce("method", F.lit("DIRECT")))
)

result = (
    cleaned
    .filter(~F.col("status").isin(["REFUNDED", "SUCCESS"]))
    .select(
        "id",
        "method",
        "paid_at",
        "amount_int",
        F.max("amount_int").over(w_ordered).alias("last7_max_amount_int"),
        F.avg("amount_int").over(w_ordered).alias("last7_avg_amount_int"),
        F.min("amount_int").over(w_ordered).alias("last7_min_amount_int"),
        F.count("amount_int").over(w2).alias("paid_at_year_count_amount_int"),
    )
)
