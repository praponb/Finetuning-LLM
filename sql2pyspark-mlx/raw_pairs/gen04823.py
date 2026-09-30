from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("launch_date").desc(), "id")

result = (
    df_products
    .filter(F.col("price") <= 610)
    .select("id", "supplier_id", "launch_date", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "supplier_id", "launch_date")
    .orderBy(F.col("id").desc(), "launch_date")
)
