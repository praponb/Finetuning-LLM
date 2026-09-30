from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating")

result = (
    df_suppliers
    .filter(~F.col("country").isin(["UK", "DE"]))
    .select("id", "country", "rating", F.rank().over(w).alias("rating_rank"))
    .orderBy(F.col("rating").desc())
)
