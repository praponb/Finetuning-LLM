from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")

result = (
    df_suppliers
    .select("id", "city", "rating", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 5)
    .orderBy("city")
)
