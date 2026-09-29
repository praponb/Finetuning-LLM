from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .filter(F.col("country").isin(["US", "UK", "JP"]))
    .select("id", "city", "rating", F.dense_rank().over(w).alias("dense_rnk"))
)
