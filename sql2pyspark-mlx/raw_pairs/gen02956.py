from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("amount_tier").orderBy(F.col("avg_amount").desc())

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 4400, "LARGE").otherwise("SMALL").alias("amount_tier"),
        F.date_trunc("YEAR", "txn_time").alias("year_start"),
    )
    .agg(
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .filter(F.col("num_txn_types") >= 1)
    .withColumn("avg_amount_rank", F.rank().over(w))
)
