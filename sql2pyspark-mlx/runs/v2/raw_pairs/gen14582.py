from pyspark.sql import functions as F

result = (
    df_products
    .filter(((F.col("category") != "Home") & F.col("supplier_id").isNull()) | (F.col("cost") >= 180))
    .groupBy("brand")
    .agg(
        F.count("*").alias("num_products"),
        F.max("launch_date").alias("last_launch_date"),
        F.round(F.stddev("price"), 2).alias("stddev_price"),
    )
    .orderBy(F.col("brand").desc())
)
