from pyspark.sql import functions as F

src = df_accounts.filter(F.col("customer_id").isNotNull())

agg = (
    src
    .groupBy("status")
    .agg(
        F.sum("balance").alias("total_balance"),
        F.avg("balance").alias("avg_balance"),
    )
)

result = agg.orderBy(F.col("total_balance").desc())
