from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("price").desc(), "id")

result = (
    df_products
    .select("id", "brand", "price", "launch_date", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
)
