from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .filter(F.col("rating") < 4.1)
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") <= 5)
)
