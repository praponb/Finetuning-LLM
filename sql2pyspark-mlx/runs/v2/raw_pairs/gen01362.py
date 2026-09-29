from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.month("order_date").isin([3, 4, 5]) | F.col("channel").isin(["partner", "store"]))
    .groupBy(F.year("order_date").alias("order_year"))
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(((100 * F.sum(F.when(F.col("channel") == "store", 1).otherwise(0))) / F.count("*")), 1).alias("pct_store"),
    )
    .filter(F.col("pct_store") > 27)
)
