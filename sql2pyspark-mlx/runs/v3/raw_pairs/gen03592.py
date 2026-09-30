from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("launch_date").desc())

result = (
    df_products
    .select("id", "brand", "launch_date", F.dense_rank().over(w).alias("pos"))
    .filter(F.col("pos") == 1)
    .select("id", "brand", "launch_date")
    .orderBy("launch_date")
)
