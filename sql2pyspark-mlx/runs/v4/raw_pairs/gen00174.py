from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("balance_int_tier").orderBy(F.col("balance_int_per_frozen").desc())

filtered = (
    df_accounts
    .filter(F.col("opened_date") < "2024-06-25")
    .select("*", F.col("balance").cast("int").alias("balance_int"))
)

result = (
    filtered
    .groupBy(
        F.when(F.col("balance_int") >= 73, "PREMIUM").otherwise("STANDARD").alias("balance_int_tier"),
        "customer_id",
    )
    .agg(
        F.round(F.sum("balance_int") / F.nullif(F.count(F.when(F.col("status") == "FROZEN", 1)), F.lit(0)), 2).alias("balance_int_per_frozen"),
        F.countDistinct("status").alias("distinct_status"),
        F.stddev_samp("balance").alias("balance_stddev"),
        F.count_if(F.col("account_type") == "CHECKING").alias("checking_events"),
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_rows"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
    )
    .withColumn("balance_int_per_frozen_dense_rank", F.dense_rank().over(w))
)
