from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.col("quantity") >= 190, "high").when(F.col("quantity") >= 41, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.max("quantity").alias("max_quantity"))
    .filter(F.col("max_quantity") > 100)
)
