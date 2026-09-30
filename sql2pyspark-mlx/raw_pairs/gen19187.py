from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        "method",
        F.when(F.col("status").isin(["REFUNDED", "SUCCESS"]), "priority").otherwise("other").alias("status_group"),
    )
    .agg(F.sort_array(F.collect_set("method")).alias("method_list"))
    .orderBy("method")
)
