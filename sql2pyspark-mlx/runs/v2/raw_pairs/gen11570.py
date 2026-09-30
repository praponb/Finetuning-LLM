from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") >= 570) & (F.col("amount") > 1010))
    .select(
        "id",
        "amount",
        "status",
        F.substring("method", 1, 3).alias("method_prefix"),
    )
    .orderBy("amount", "method_prefix")
)
