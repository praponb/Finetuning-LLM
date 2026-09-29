from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").desc_nulls_last(), "id")

valid = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2024-04-30 01:00:00").cast("timestamp")) & F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        F.floor("amount").alias("amount_whole"),
    )
)

result = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)
