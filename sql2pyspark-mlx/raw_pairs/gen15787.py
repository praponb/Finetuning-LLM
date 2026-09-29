from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft")

result = (
    df_stores
    .select(
        "id",
        "city",
        "sqft",
        "opened_date",
        F.dense_rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") == 1)
    .select("id", "city", "sqft", "opened_date")
    .orderBy("opened_date")
)
