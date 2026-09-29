from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("opened_date").desc())

result = (
    df_stores
    .filter(F.col("opened_date") > "2023-11-07")
    .select("id", "city", "opened_date", "region", F.dense_rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .select("id", "city", "opened_date", "region")
)
