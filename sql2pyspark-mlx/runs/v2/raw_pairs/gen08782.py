from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue", "id")

result = (
    df_store_sales
    .select("id", "product_id", "revenue", "units", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 1)
    .orderBy(F.col("units").desc())
)
