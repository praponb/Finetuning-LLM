from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date")

result = (
    df_products
    .filter(F.col("brand").isin(["Umbrella", "Acme", "Stark"]))
    .select("id", "category", "launch_date", F.dense_rank().over(w).alias("dense_rnk"))
    .orderBy(F.col("category").desc())
)
