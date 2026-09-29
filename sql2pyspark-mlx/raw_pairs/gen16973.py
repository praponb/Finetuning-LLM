from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")

result = (
    df_products
    .withColumn("avg_cost_by_category", F.avg("cost").over(w))
    .filter(F.col("cost") < F.col("avg_cost_by_category"))
    .select("id", "category", "cost")
    .orderBy("category", F.col("id").desc())
)
