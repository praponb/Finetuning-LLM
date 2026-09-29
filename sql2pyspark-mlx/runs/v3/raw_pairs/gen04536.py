from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isNull())
    .groupBy("method")
    .agg(
        F.max("amount").alias("max_amount"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
    )
    .filter(F.col("max_amount") > 840)
)
