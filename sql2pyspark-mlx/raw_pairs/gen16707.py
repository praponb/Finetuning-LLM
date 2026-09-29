from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_stores
    .filter(F.col("opened_date") > "2023-11-01")
    .select("id", "region", "opened_date", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") == 1)
    .select("id", "region", "opened_date")
    .orderBy("region")
)
