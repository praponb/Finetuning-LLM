from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("city_group")

result = (
    df_suppliers
    .filter(F.col("rating") <= 2.7)
    .groupBy(
        F.when(F.col("city").isin(["London", "Tokyo"]), "core").otherwise("other").alias("city_group"),
    )
    .agg(F.sum("rating").alias("total_rating"))
    .withColumn("cumulative_rating", F.sum("total_rating").over(w))
)
