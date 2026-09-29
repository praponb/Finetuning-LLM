from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.col("rating") < 4.1)
    .select("id", "city", "rating", F.row_number().over(w).alias("rating_row"))
    .orderBy(F.col("rating").desc())
)
