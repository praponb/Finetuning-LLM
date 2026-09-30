from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("cost").desc(), "id")

result = (
    df_products
    .filter(F.lower("product_name").like("%esk%"))
    .select("id", "brand", "cost", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 1)
    .select("id", "brand", "cost")
    .orderBy("cost")
)
