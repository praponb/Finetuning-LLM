from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id")

result = (
    df_stores
    .filter(F.col("sqft") >= 9961)
    .select("id", "region", "opened_date", F.ntile(4).over(w).alias("quartile"))
    .orderBy(F.col("quartile").desc())
)
