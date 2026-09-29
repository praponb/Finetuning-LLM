from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")

result = (
    df_suppliers
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 3)
    .orderBy(F.col("id").desc())
)
