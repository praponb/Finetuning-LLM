from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "right")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("p.product_name").like("Des%"))
    .groupBy(
        F.when(F.col("s.city").isin(["Berlin", "Singapore"]), "core").otherwise("other").alias("city_group"),
    )
    .agg(F.sum(F.col("i.quantity")).alias("total_quantity"))
    .orderBy(F.col("total_quantity").desc(), F.col("city_group").desc())
)
