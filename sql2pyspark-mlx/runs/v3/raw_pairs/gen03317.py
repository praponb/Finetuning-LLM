from pyspark.sql import functions as F

src = df_transactions.filter((F.col("amount") <= 1400) & F.col("amount").isNotNull())

agg = (
    src
    .groupBy(
        F.when(F.col("amount") >= 4200, "high").when(F.col("amount") >= 1500, "mid").otherwise("low").alias("amount_bucket"),
        F.month("txn_time").alias("txn_month"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
)

result = agg.select("amount_bucket", "txn_month", "merchant_values")
