from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("amount") > 1000) & F.col("shipping_fee").between(23, 32))
    .groupBy("status")
    .agg(
        F.count("*").alias("num_orders"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        (F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"),
    )
)
