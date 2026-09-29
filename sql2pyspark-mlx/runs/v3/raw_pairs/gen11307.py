from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.col("city").isin(["New York", "Berlin"]))
    .select("id", "country", "rating", "city", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 2)
    .select("id", "country", "rating", "city")
)
