from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("unit_price").between(300, 320))
    .withColumn("quantity_band", F.when(F.col("quantity") >= 7, "High").when(F.col("quantity") >= 4, "Medium").otherwise("Low"))
)

agg = (
    base
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
        "order_id",
    )
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = (
    agg
    .select("quantity_bucket", "order_id")
    .orderBy(F.col("quantity_bucket").desc(), F.col("order_id").desc())
)
