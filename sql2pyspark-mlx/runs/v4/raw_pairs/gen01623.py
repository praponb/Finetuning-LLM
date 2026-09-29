from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units").desc(), "id")

result = (
    df_store_sales
    .select("id", "product_id", "units", "revenue", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .orderBy(F.col("units").desc())
)
