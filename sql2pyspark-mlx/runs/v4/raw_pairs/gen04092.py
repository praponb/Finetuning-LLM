from pyspark.sql import functions as F

src = (
    df_order_items
    .filter(F.col("order_id").isin([16, 19, 31]))
    .withColumn("line_total", (F.col("unit_price") * F.col("quantity")))
)

agg = (
    src
    .groupBy("order_id")
    .agg(
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
        F.min("unit_price").alias("min_unit_price"),
    )
)

result = agg.orderBy(F.col("revenue").desc())
