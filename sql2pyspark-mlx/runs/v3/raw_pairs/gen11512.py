from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .select("id", "country", "rating", F.row_number().over(w).alias("row_num"))
    .orderBy(F.col("rating").desc())
)
