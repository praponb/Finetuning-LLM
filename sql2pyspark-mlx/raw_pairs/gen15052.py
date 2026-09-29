from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") != "SHIPPED")
    .groupBy(F.year("order_date").alias("order_year"), "status")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.avg("discount"), 2).alias("avg_discount"),
    )
    .filter(F.col("avg_discount") < 37)
    .orderBy(F.col("order_year").desc())
)
