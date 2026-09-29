from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id")

result = (
    df_stores
    .filter(F.col("sqft").between(5356, 6349))
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 3)
    .select("id", "region", "opened_date")
)
