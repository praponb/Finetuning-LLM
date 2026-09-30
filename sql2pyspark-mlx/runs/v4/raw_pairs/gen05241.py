from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")

result = (
    df_suppliers
    .filter(~F.col("country").isin(["DE", "SG", "UK"]))
    .select("id", "country", "rating", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 5)
    .select("id", "country", "rating")
    .orderBy(F.col("id").desc(), F.col("rating").desc())
)
