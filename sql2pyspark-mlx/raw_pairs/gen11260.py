from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .filter(F.col("city") == "New York")
    .select("id", "city", "rating", "country", F.rank().over(w).alias("row_num"))
    .filter(F.col("row_num") == 1)
)
