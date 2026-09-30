from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id")
w2 = Window.partitionBy("region").orderBy("opened_date", "id")

result = (
    df_stores
    .filter(F.col("store_name").isNotNull())
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        "region",
        F.sum("sqft").over(w).alias("running_sqft"),
        F.sum("sqft").over(w2).alias("running_sqft_2"),
    )
)
