from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([22, 26, 30]))
    .groupBy(
        F.when(F.col("amount") >= 3700, "high").when(F.col("amount") >= 1200, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Lazada", 1).otherwise(0))) / F.count("*")), 1).alias("pct_lazada"),
    )
)
