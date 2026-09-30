from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") <= "2024-03-01")
    .select(
        "end_date",
        "status",
        F.when(F.col("status").isin(["EXPIRED", "PAUSED"]), "Core").otherwise("Other").alias("status_group"),
    )
)
