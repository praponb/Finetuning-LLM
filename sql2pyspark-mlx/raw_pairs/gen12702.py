from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .select("id", "city", "rating", F.row_number().over(w).alias("rating_row"))
)
