from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        "status",
        F.when(F.col("method").isin(["bank_transfer", "card"]), "group_a").otherwise("other").alias("method_group"),
    )
    .agg(F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"))
)
