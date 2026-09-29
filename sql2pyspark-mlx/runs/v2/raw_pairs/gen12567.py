from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .filter(F.length("supplier_name") >= 8)
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") <= 3)
    .orderBy("city")
)
