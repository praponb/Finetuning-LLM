from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("age", "id")

result = (
    df_customers
    .filter(F.col("name").like("%Chaiy%"))
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 3)
)
