from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_inventory
    .filter(F.col("last_restock") <= "2025-01-01")
    .groupBy(
        F.when(F.col("quantity") >= 199, "high").when(F.col("quantity") >= 117, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.count("*").alias("cnt"),
    )
    .withColumn("pct_of_total", F.round(((F.col("total_quantity") * 100) / F.sum("total_quantity").over(w)), 2))
    .orderBy(F.col("cnt").desc())
)
