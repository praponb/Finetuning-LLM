from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(
        (F.max("discount") - F.min("discount")).alias("discount_range"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .orderBy(F.col("order_quarter").desc(), F.col("discount_range").desc())
)
