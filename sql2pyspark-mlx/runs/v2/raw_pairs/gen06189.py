from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_rating").desc())

result = (
    df_suppliers
    .filter(~F.col("supplier_name").like("%EuroGoo%"))
    .groupBy("country")
    .agg(F.sum("rating").alias("total_rating"))
    .withColumn("rnk", F.rank().over(w))
    .orderBy("rnk")
)
