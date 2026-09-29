from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("txn_time"))
w2 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

enriched = (
    df_transactions
    .filter(F.col("amount") <= 1100)
    .select(
        "*",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.sum("amount").over(w2).alias("amount_sum_90d"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("amount_sum_90d") >= 19787, "PREMIUM").otherwise("STANDARD").alias("amount_sum_90d_tier"),
    )
    .agg(
        F.max("txn_time").alias("last_seen"),
        F.count(F.when(F.col("merchant") == "Starbucks", 1)).alias("starbucks_rows"),
        F.max(F.when(F.col("merchant") == "Amazon", F.col("amount_sum_90d"))).alias("amazon_max_amount_sum_90d"),
    )
    .orderBy(F.col("amazon_max_amount_sum_90d").desc_nulls_first())
)
