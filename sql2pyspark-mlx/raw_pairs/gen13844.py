from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("cost").desc(), "id")

result = (
    df_products
    .select("id", "brand", "cost", "launch_date", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 1)
    .select("id", "brand", "cost", "launch_date")
    .orderBy(F.col("id").desc())
)
