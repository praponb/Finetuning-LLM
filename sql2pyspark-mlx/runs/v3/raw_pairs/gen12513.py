from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy("opened_date", "id")
w2 = Window.partitionBy("city")

result = (
    df_stores
    .select(
        "id",
        "region",
        "city",
        "opened_date",
        "sqft",
        (F.col("sqft") - F.lag("sqft", 1, 0).over(w)).alias("sqft_change"),
        F.count("*").over(w2).alias("city_count"),
    )
    .orderBy("opened_date")
)
