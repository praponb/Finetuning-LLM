from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy("amount", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id")
w4 = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("method").orderBy(F.col("amount").desc())

filtered = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select(
        "*",
        F.concat_ws("-", "method", "status").alias("method_status_key"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("method_clean"),
    )
)

result = (
    filtered
    .select(
        "*",
        F.avg("amount").over(w).alias("amount_avg_5_prior"),
        F.count("amount").over(w2).alias("amount_count_30d"),
        F.row_number().over(w3).alias("seq_no"),
        F.sum("amount").over(w4).alias("running_amount"),
        F.percent_rank().over(w5).alias("amount_percent_rank"),
    )
)
