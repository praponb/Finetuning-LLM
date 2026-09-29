from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "method").orderBy(F.col("is_bank_transfer").asc_nulls_last())

cleaned = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-08-27").cast("date")) & (F.col("method") == "promptpay"))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.lower("method").alias("method_lower"),
        F.when(F.col("method") == "bank_transfer", 1).otherwise(0).alias("is_bank_transfer"),
    )
)

result = (
    cleaned
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "paid_at", "method_lower", "is_bank_transfer")
)
