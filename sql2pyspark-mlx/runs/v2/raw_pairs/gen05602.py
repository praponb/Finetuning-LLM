from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("launch_date").desc(), "id")

result = (
    df_products
    .filter(F.year("launch_date") >= 2023)
    .select("id", "category", "launch_date", F.row_number().over(w).alias("row_num"))
    .orderBy(F.col("row_num").desc())
)
