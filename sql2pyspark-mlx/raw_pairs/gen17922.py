from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "right")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.brand").isNull())
    .groupBy(
        F.when(F.col("s.country").isin(["DE", "JP"]), "group_a").otherwise("other").alias("country_group"),
        F.col("s.city"),
    )
    .agg(F.round(F.stddev(F.col("oi.unit_price")), 2).alias("stddev_unit_price"))
    .filter(F.col("stddev_unit_price") > 30)
    .orderBy("country_group", F.col("city").desc())
)
