from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").isNull())
    .agg(
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.avg("amount").alias("avg_amount"),
    )
)
