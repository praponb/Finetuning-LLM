from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.percentile_approx("shipping_fee", [0.25, 0.5, 0.75]).alias("shipping_fee_quartiles"),
        F.round(F.sum("discount") / F.nullif(F.count(F.when(F.col("channel") == "web", 1)), F.lit(0)), 2).alias("discount_per_web"),
    )
)
