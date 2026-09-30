from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_stores
    .select("id", "city", "opened_date", "region", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
    .select("id", "city", "opened_date", "region")
)
