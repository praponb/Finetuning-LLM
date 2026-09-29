from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id")

result = (
    df_stores
    .filter(F.col("opened_date").between("2023-08-01", "2025-05-05"))
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") == 1)
    .select("id", "city", "opened_date")
)
