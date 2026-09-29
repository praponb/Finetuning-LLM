from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date")

result = (
    df_products
    .select("id", "category", "launch_date", F.rank().over(w).alias("rnk"))
    .orderBy("rnk")
)
