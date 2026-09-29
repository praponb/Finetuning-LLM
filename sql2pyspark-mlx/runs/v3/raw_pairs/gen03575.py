from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        "store_id",
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
    )
)
