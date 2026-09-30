from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .groupBy(
        F.when(F.col("suppliers.city").isin(["London", "Bangkok"]), "core").otherwise("other").alias("city_group"),
    )
    .agg(F.round(F.stddev(F.col("products.price")), 2).alias("stddev_price"))
)
