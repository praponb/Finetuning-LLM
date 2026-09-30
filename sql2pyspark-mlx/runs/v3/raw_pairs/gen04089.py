from pyspark.sql import functions as F

src = df_payments.filter(F.col("method").isin(["bank_transfer", "card"]))

agg = (
    src
    .groupBy(
        F.when(F.col("status").isin(["SUCCESS", "FAILED"]), "tier_1").otherwise("other").alias("status_group"),
    )
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = agg.select("status_group", "avg_amount")
