from pyspark.sql import functions as F

result = (
    df_inventory
    .cube(
        "store_id",
        F.when(F.col("quantity") >= 251, "high").when(F.col("quantity") >= 76, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.sum("quantity").alias("total_quantity"))
    .orderBy(F.col("store_id").desc())
)
