from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")

result = (
    df_products
    .filter(F.array_contains("tags", "sale"))
    .select("id", "category", "cost", F.avg("cost").over(w).alias("category_avg_cost"))
)
