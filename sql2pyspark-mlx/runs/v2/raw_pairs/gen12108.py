from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_units").desc())

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("units") >= 35, "high").when(F.col("units") >= 17, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(F.sum("units").alias("total_units"))
    .withColumn("rnk", F.rank().over(w))
    .orderBy("rnk", "units_bucket")
    .limit(5)
)
