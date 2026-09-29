from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating")

result = (
    df_suppliers
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") <= 2)
    .select("id", "city", "rating", "supplier_name")
)
