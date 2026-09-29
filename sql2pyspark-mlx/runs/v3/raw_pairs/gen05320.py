from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        (F.max("discount") - F.min("discount")).alias("discount_range"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .filter(F.col("discount_range") >= 22)
    .select("status", "discount_range", "status_list")
)
