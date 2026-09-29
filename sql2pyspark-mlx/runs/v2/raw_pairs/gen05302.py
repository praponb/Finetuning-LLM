from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")

result = (
    df_suppliers
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 3)
    .orderBy("country")
)
