from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy(F.col("paid_at_month").desc(), "id")

valid = (
    df_payments
    .filter(F.col("paid_at") > F.lit("2023-10-29 06:00:00").cast("timestamp"))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.concat("method", F.lit(" - "), "status").alias("method_status"),
        F.month("paid_at").alias("paid_at_month"),
    )
)

result = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "order_id", "method", "paid_at")
)
