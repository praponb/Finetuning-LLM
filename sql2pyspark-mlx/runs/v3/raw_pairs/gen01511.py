from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.col("country").isin(["DE", "UK", "JP"]))
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 2)
    .select("id", "country", "rating", "supplier_name")
    .orderBy("rating", F.col("supplier_name").desc())
)
