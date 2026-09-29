from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").between(740, 1030))
    .groupBy(
        F.date_trunc("MONTH", "order_date").alias("order_month"),
        "channel",
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        (F.max("discount") - F.min("discount")).alias("discount_range"),
    )
    .filter(F.col("discount_range") >= 21)
    .orderBy("status_values")
)
