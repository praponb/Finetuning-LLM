from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("unit_price").between(150, 250))
    .withColumn("line_total", (F.col("quantity") * F.col("unit_price")))
)

agg = (
    base
    .groupBy(
        "order_id",
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.count("*").alias("total_count"))
)

result = (
    agg
    .orderBy(F.col("quantity_bucket").desc(), "order_id")
    .limit(10)
)
