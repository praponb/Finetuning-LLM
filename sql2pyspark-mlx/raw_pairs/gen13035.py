from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date", "id")

result = (
    df_products
    .filter(F.col("price") > 710)
    .select("id", "category", "launch_date", F.row_number().over(w).alias("rn"))
    .orderBy(F.col("rn").desc())
)
