from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("amount_tier").orderBy(F.col("cnt").desc())

kept_keys = (
    df_payments
    .select("method")
    .exceptAll(
        df_payments
        .filter(F.hour("paid_at").between(9, 17))
        .select("method")
    )
)

reconciled = (
    df_payments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.method") == F.col("k.method")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("amount") >= 1690, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
        "method",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .withColumn("cnt_percent_rank", F.percent_rank().over(w))
    .orderBy(F.col("amount_tier").desc())
)
