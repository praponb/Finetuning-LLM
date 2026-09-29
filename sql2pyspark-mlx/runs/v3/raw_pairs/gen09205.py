from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_stores
    .select("id", "city", "opened_date", "region", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .select("id", "city", "opened_date", "region")
    .orderBy("city", F.col("region").desc())
)
