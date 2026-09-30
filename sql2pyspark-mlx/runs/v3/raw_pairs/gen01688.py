from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("cost").desc(), "id")

result = (
    df_products
    .filter(F.col("cost") > 210)
    .select("id", "category", "cost", F.row_number().over(w).alias("rn"))
    .orderBy("category", "cost")
)
