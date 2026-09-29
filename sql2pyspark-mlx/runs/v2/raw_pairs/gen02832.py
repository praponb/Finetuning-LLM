from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units").desc(), "id")

result = (
    df_store_sales
    .filter(F.col("sale_date") > "2025-08-07")
    .select("id", "product_id", "units", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 2)
    .select("id", "product_id", "units")
    .orderBy(F.col("id").desc())
)
