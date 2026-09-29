from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating")

result = (
    df_suppliers
    .select("id", "city", "rating", F.percent_rank().over(w).alias("rating_pct_rank"))
)
