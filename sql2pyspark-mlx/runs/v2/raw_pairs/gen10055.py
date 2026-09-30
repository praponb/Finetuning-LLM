from pyspark.sql import functions as F

src = (
    df_order_items
    .filter(F.col("quantity").between(5, 8) & F.col("unit_price").between(120, 200) & (F.col("unit_price") >= 200))
)

agg = (
    src
    .groupBy("order_id")
    .agg(
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
    )
)

result = agg.select("order_id", "revenue", "stddev_unit_price")
