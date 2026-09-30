from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant")
w2 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)

with_windows = (
    df_transactions
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w)), 4))
    .withColumn("amount_count_7d", F.count("amount").over(w2))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("amount_count_7d") >= 13, "PREMIUM").otherwise("STANDARD").alias("amount_count_7d_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.sum(F.when(F.col("merchant") == "Amazon", F.col("amount_share")).otherwise(0)).alias("amazon_amount_share"),
    )
    .orderBy(F.col("amount_count_7d_tier").desc_nulls_first())
)
