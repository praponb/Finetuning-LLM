from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_suppliers
    .filter(F.col("rating").between(3.0, 3.1))
    .groupBy(
        F.when(F.col("country").isin(["US", "UK"]), "core").otherwise("other").alias("country_group"),
    )
    .agg(
        F.sum("rating").alias("total_rating"),
        F.count("*").alias("cnt"),
    )
    .withColumn("pct_of_total", F.round(((F.col("total_rating") * 100) / F.sum("total_rating").over(w)), 2))
)
