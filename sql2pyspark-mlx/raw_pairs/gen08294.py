from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["promptpay", "paypal"]) | (F.col("status") == "SUCCESS"))
    .groupBy(F.quarter("paid_at").alias("paid_quarter"))
    .agg(
        F.countDistinct("amount").alias("unique_methods"),
        F.round(((100 * F.sum(F.when(F.col("method") == "paypal", 1).otherwise(0))) / F.count("*")), 1).alias("pct_paypal"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .orderBy("unique_methods", "paid_quarter")
    .limit(50)
)
