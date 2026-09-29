from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Tra%"))
    .groupBy("country")
    .agg(
        F.sum("rating").alias("total_rating"),
        F.count("*").alias("cnt"),
    )
    .withColumn("pct_of_total", F.round(((F.col("total_rating") * 100) / F.sum("total_rating").over(w)), 2))
    .orderBy(F.col("total_rating").desc(), "country")
    .limit(50)
)
