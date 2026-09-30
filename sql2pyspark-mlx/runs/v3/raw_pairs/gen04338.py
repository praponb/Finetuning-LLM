from pyspark.sql import functions as F

filtered_orders = (
    df_orders
    .filter(((F.col("amount") <= 1520) & F.col("shipping_fee").between(26, 35)) | (F.col("amount") >= 790))
)

agg = (
    filtered_orders
    .groupBy("store_id")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.min("amount").alias("min_amount"),
    )
)

result = agg.filter(F.col("min_amount") > 810)
