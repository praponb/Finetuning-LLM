from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_suppliers
    .filter(F.col("city") == "Bangkok")
    .groupBy(
        F.when(F.col("rating") >= 4.2, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.sum("rating").alias("total_rating"))
    .withColumn("pct_of_total", F.round(((F.col("total_rating") * 100) / F.sum("total_rating").over(w)), 2))
    .orderBy("pct_of_total")
)
