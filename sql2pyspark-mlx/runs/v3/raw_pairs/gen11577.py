from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.countDistinct("discount").alias("unique_customers"),
    )
    .filter(F.col("unique_customers") < 4)
    .orderBy("status")
)
