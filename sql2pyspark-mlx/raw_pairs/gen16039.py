from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating")

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("Gl%"))
    .select("id", "rating", F.dense_rank().over(w).alias("rating_dense_rank"))
    .orderBy("id", "rating")
)
