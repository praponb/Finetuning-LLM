from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .filter(F.col("rating") <= 1.8)
    .select("id", "city", "rating", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 2)
    .orderBy(F.col("rating").desc())
)
