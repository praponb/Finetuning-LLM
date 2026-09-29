from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_stores
    .filter(F.col("sqft") <= 8244)
    .select("id", "region", "opened_date", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .select("id", "region", "opened_date")
    .orderBy("opened_date")
)
