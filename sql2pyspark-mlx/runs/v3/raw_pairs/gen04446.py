from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc())

result = (
    df_stores
    .select("id", "city", "sqft", F.percent_rank().over(w).alias("sqft_pct_rank"))
    .orderBy(F.col("sqft").desc(), "id")
)
