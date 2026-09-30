from pyspark.sql import functions as F

src = (
    df_order_items
    .filter(F.col("order_id").isin([7, 28]) & (F.col("product_id").isin([16, 32]) | (F.col("quantity") >= 6)))
)

summary = (
    src
    .groupBy("product_id")
    .agg(F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"))
)

result = summary.filter(F.col("stddev_unit_price") > 66)
