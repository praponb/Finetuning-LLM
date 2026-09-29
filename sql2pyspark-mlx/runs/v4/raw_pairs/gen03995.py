from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc_nulls_first())

valid = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(90, 150))
    .select(
        "*",
        F.when(F.col("status").isin(["PAUSED", "EXPIRED"]), "Tier 1").otherwise("Other").alias("status_group"),
    )
)

result = (
    valid
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)
