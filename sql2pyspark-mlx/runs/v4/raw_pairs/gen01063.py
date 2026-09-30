from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_amount").desc())

result = (
    df_transactions
    .groupBy(
        F.when(F.col("merchant").isin(["Amazon", "Grab"]), "group_a").otherwise("other").alias("merchant_group"),
    )
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("amount_rank", F.rank().over(w))
    .orderBy("merchant_group", F.col("total_amount").desc())
)
