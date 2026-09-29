from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("units")

result = (
    df_store_sales
    .filter(F.col("sale_date") < "2024-10-27")
    .select("id", "units", F.dense_rank().over(w).alias("units_dense_rank"))
)
