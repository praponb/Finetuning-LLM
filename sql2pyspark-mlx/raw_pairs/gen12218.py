from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .filter(F.col("rating").between(2.1, 3.4))
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 5)
)
