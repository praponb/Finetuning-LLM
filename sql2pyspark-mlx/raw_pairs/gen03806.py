from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("amount_tier").orderBy(F.col("max_amount").desc())

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 3900, "HIGH").otherwise("LOW").alias("amount_tier"),
        "account_id",
    )
    .agg(
        F.max("amount").alias("max_amount"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .withColumn("max_amount_percent_rank", F.percent_rank().over(w))
)
