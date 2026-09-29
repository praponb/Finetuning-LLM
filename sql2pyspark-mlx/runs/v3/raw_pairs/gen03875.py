from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc())

result = (
    df_stores
    .select("id", "region", "opened_date", "city", F.rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 2)
    .orderBy("city", "opened_date")
)
