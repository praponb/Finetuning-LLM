from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy(F.month("opened_date")).orderBy(F.col("sqft").desc(), "id")

result = (
    df_stores
    .select(
        "id",
        "city",
        "sqft",
        "opened_date",
        F.max("sqft").over(w).alias("city_max_sqft"),
        F.ntile(2).over(w2).alias("quartile"),
    )
    .orderBy(F.col("id").desc())
)
