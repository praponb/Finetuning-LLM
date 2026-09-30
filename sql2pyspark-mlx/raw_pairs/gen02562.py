from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("opened_date").desc_nulls_last(), "id")

latest = (
    df_accounts
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .groupBy(
        F.when(F.col("balance") >= 67000, "HIGH").otherwise("LOW").alias("balance_tier"),
    )
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.max(F.when(F.col("account_type") == "SAVINGS", F.col("balance"))).alias("savings_max_balance"),
    )
)
