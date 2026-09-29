from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("quarter_start").orderBy(F.col("avg_balance").desc())

result = (
    df_accounts
    .groupBy(F.date_trunc("QUARTER", "opened_date").alias("quarter_start"), "status")
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.round(F.avg("balance"), 2).alias("avg_balance_2"),
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"),
    )
    .withColumn("avg_balance_rank", F.rank().over(w))
    .orderBy(F.col("avg_balance").desc())
)
