from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.min("launch_date").alias("first_launch_date"),
        F.count(F.when(F.col("brand") == "Acme", 1)).alias("acme_count"),
    )
    .filter(F.col("acme_count") < 7)
    .select("supplier_id", "first_launch_date", "acme_count")
)
