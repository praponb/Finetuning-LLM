from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("units").desc(), "id")

result = (
    df_store_sales
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 5)
    .orderBy(F.col("revenue").desc())
)
