from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")

result = (
    df_suppliers
    .select("id", "country", "rating", "city", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .orderBy(F.col("city").desc(), "pos")
)
