from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("opened_date").desc())
w2 = Window.partitionBy("region")

result = (
    df_stores
    .select(
        "id",
        "opened_date",
        "region",
        "sqft",
        F.rank().over(w).alias("rnk"),
        F.min("sqft").over(w2).alias("region_min_sqft"),
    )
    .orderBy("region")
)
