from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("units").desc(), "id")

result = (
    df_store_sales
    .filter(F.col("sale_date") < "2025-09-14")
    .select("id", "store_id", "units", "product_id", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
    .select("id", "store_id", "units", "product_id")
    .orderBy(F.col("id").desc())
)
