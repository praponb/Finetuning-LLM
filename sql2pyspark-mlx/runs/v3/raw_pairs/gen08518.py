from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .orderBy("avg_amount", "method")
    .limit(10)
)
