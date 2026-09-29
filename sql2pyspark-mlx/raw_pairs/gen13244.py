from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") <= 600) & (F.col("amount") <= 1450))
    .groupBy(F.to_date("paid_at").alias("paid_day"), "status")
    .agg(
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("min_amount") < 1120)
    .orderBy(F.col("status").desc(), "paid_day")
    .limit(50)
)
