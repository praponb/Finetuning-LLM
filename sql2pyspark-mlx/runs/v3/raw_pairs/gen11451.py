from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_stores
    .select("id", "region", "opened_date", "city", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 5)
    .orderBy(F.col("id").desc())
)
