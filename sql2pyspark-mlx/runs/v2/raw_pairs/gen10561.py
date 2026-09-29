from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("cost", "id")

result = (
    df_products
    .filter(F.array_contains("tags", "eco"))
    .select("id", "brand", "cost", "supplier_id", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "brand", "cost", "supplier_id")
)
