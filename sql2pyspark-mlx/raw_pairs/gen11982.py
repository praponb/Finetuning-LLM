from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy("amount", "id")
w3 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy(F.year("paid_at"), "order_id").orderBy("amount", "id").rowsBetween(-3, -1)

filtered = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-08-18 17:00:00").cast("timestamp")) & F.col("order_id").isNotNull())
    .withColumn("method_status_key", F.concat_ws(",", "method", "status"))
    .withColumn("resolved_method", F.coalesce("method", F.lit("NONE")))
)

result = (
    filtered
    .select(
        "id",
        "order_id",
        "status",
        F.avg("amount").over(w).alias("amount_avg_3"),
        F.row_number().over(w2).alias("row_seq"),
        F.sum("amount").over(w3).alias("running_amount"),
        F.avg("amount").over(w_recent).alias("amount_avg_3_prior"),
    )
)
