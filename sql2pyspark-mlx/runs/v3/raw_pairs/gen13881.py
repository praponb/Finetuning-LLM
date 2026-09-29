from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .groupBy(
        F.when(F.year(F.col("o.order_date")) < 2024, "before_2024").when(F.year(F.col("o.order_date")) == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
        F.when(F.col("o.shipping_fee") >= 38, "high").when(F.col("o.shipping_fee") >= 7, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(F.sum(F.col("oi.unit_price")).alias("total_unit_price"))
    .orderBy("order_cohort")
)
