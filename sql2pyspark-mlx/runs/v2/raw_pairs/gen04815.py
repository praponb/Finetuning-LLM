from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("method") != "cash") & (F.col("amount") > 820))
    .groupBy("method")
    .agg(
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.min("paid_at").alias("first_paid_at"),
        F.max("amount").alias("max_amount"),
    )
    .orderBy("method")
)
