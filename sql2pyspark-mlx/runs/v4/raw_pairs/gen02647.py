from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "amount",
        "status",
        "method",
        F.initcap(F.trim("method")).alias("method_title"),
        F.when(F.col("method") == "promptpay", 1).otherwise(0).alias("is_promptpay"),
    )
    .orderBy("method_title")
)
