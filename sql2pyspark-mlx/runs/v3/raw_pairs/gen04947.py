from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .filter(F.col("rating") <= 3.3)
    .select("id", "rating", F.rank().over(w).alias("rating_rank"))
    .orderBy(F.col("rating").desc())
)
