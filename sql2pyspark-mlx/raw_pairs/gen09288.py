from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("txn_time").desc(), "id")

latest = (
    df_transactions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .groupBy(
        F.when(F.col("amount") >= 3000, "LARGE").otherwise("SMALL").alias("amount_tier"),
    )
    .agg(
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("txn_type") == "FEE", 1)), F.lit(0)), 2).alias("amount_per_fee"),
        F.count_if(F.col("merchant") == "Shell").alias("shell_events"),
        F.min("amount").alias("min_amount"),
        F.min("amount").alias("min_amount_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("merchant") == "Lazada", F.col("amount")).otherwise(0)).alias("lazada_amount"),
    )
    .orderBy(F.col("shell_events").desc_nulls_last())
)
