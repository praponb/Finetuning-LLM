from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("days_since_paid_at").desc_nulls_last(), "id")

filtered = (
    df_payments
    .filter((F.col("paid_at") >= F.to_timestamp(F.lit("2023-10-24 11:00:00"))) & F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.datediff(F.current_date(), "paid_at").alias("days_since_paid_at"),
    )
)

result = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "method", "amount", "paid_at")
)
