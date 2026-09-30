from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")

result = (
    df_stores
    .filter(F.col("city").isin(["Berlin", "New York"]))
    .select(
        "id",
        "city",
        "sqft",
        "region",
        F.ntile(3).over(w).alias("bucket"),
        F.first("city").over(w2).alias("first_city"),
    )
)
