from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("price").desc(), "id")

result = (
    df_products
    .filter(F.col("launch_date") >= "2024-03-01")
    .select("id", "brand", "price", "supplier_id", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") == 1)
    .orderBy(F.col("price").desc(), F.col("supplier_id").desc())
)
