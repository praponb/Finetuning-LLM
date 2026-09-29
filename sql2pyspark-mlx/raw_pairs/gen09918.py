from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy(F.col("adjusted_amount").asc_nulls_last(), "id")

filtered = (
    df_payments
    .filter(F.col("method").eqNullSafe(F.lit("promptpay")))
    .select(
        "*",
        F.round((F.col("amount") * 0.9), 2).alias("adjusted_amount"),
        F.coalesce("method", F.lit("unassigned")).alias("method_filled"),
    )
)

result = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        "adjusted_amount",
        "method_filled",
    )
)
