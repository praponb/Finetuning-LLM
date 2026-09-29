from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("city").orderBy(F.col("opened_date").desc())

result = (
    df_stores
    .filter(F.col("sqft").between(5952, 8352))
    .select(
        "id",
        "city",
        "sqft",
        "opened_date",
        F.max("sqft").over(w).alias("city_max_sqft"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
)
