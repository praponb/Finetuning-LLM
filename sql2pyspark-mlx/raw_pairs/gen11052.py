from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc())

result = (
    df_stores
    .filter(F.col("city") != "London")
    .select(
        "id",
        "region",
        "opened_date",
        "store_name",
        F.dense_rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") == 1)
    .select("id", "region", "opened_date", "store_name")
    .orderBy("opened_date")
)
