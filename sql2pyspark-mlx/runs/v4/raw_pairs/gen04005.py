from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date", "id")

result = (
    df_products
    .select("id", "category", "launch_date", F.ntile(2).over(w).alias("bucket"))
    .orderBy(F.col("id").desc())
)
