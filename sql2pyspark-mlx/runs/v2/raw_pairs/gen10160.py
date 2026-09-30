from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("cost", "id")

result = (
    df_shipments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 3)
)
