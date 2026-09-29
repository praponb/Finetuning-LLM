from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "amount",
        F.when(F.col("status").isin(["COMPLETED", "CANCELLED"]), "Core").otherwise("Other").alias("status_group"),
        F.round("amount", 2).alias("amount_rounded"),
    )
)
