from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id")

result = (
    df_stores
    .select("id", "city", "opened_date", "region", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") == 1)
    .select("id", "city", "opened_date", "region")
)
