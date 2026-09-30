from pyspark.sql import functions as F

result = (
    df_orders
    .rollup(
        "channel",
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.min("shipping_fee").alias("min_shipping_fee"),
        (F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"),
    )
)
