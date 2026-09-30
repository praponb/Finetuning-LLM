from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc())
w2 = Window.partitionBy("region", "city").orderBy("opened_date", "id")

result = (
    df_stores
    .filter(F.col("store_name").like("Do%"))
    .select(
        "id",
        "region",
        "sqft",
        "city",
        "opened_date",
        F.rank().over(w).alias("sqft_rank"),
        F.sum("sqft").over(w2).alias("running_sqft"),
    )
)
