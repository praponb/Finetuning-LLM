from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").asc_nulls_last(), "id")

latest = (
    df_transactions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .groupBy(
        F.when(F.col("amount") >= 3900, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
        "txn_type",
    )
    .agg(
        F.count_if(F.col("merchant") == "7-Eleven").alias("7_eleven_events"),
        F.count("*").alias("event_count"),
        F.min("txn_time").alias("started_at"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("merchant") == "Amazon", 1)), F.lit(0)), 2).alias("amount_per_amazon"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
    )
    .filter(F.col("avg_amount") >= 0)
)
