from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.size("tags") >= 1)
    .rollup("brand", "supplier_id")
    .agg(
        F.countDistinct("cost").alias("unique_suppliers"),
        F.max("launch_date").alias("last_launch_date"),
    )
    .orderBy("last_launch_date", F.col("brand").desc())
)
