from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "customer_id").orderBy(F.col("balance").asc_nulls_first())
w2 = Window.partitionBy("account_type").orderBy(F.col("balance").desc_nulls_last())

with_windows = (
    df_accounts
    .withColumn("balance_cume_dist", F.cume_dist().over(w))
    .withColumn("balance_cume_dist_2", F.cume_dist().over(w2))
)

result = (
    with_windows
    .rollup(
        "account_type",
        F.date_trunc("MONTH", "opened_date").alias("activity_month"),
    )
    .agg(
        F.countDistinct("account_type").alias("distinct_account_types"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("status") == "OPEN", 1)), F.lit(0)), 2).alias("balance_per_open"),
    )
    .orderBy(F.col("activity_month").desc())
)
