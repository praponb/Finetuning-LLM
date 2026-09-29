from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .filter(F.col("country").isin(["UK", "JP"]))
    .select("id", "country", "rating", F.rank().over(w).alias("rating_rank"))
)
