from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("cost")

result = (
    df_products
    .filter(F.col("brand").isin(["Initech", "Acme"]))
    .select("id", "category", "cost", "supplier_id", F.dense_rank().over(w).alias("rn"))
    .filter(F.col("rn") == 1)
    .select("id", "category", "cost", "supplier_id")
)
