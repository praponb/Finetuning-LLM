from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")

result = (
    df_suppliers
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") == 1)
    .orderBy(F.col("rating").desc())
)
