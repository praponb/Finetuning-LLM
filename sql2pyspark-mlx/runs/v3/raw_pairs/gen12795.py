from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .filter(~F.col("country").isin(["US", "UK"]))
    .select("id", "country", "rating", "city", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 2)
    .select("id", "country", "rating", "city")
)
