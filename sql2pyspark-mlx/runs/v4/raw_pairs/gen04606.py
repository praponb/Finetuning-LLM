from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("rating").desc_nulls_last(), "id")

result = (
    df_suppliers
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)
