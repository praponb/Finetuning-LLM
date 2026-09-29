from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        "order_id",
        F.when(F.col("status").isin(["FAILED", "REFUNDED"]), "core").otherwise("other").alias("status_group"),
    )
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)
