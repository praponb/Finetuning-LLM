from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") > 0.17)
    .groupBy(
        "channel",
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
        F.date_format("order_date", "yyyy-MM").alias("order_month"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.max("order_date").alias("last_order_date"),
    )
    .orderBy("order_cohort")
)
