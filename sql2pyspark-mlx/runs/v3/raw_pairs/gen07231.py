from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id")
w2 = Window.partitionBy("city")

result = (
    df_stores
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        F.lag("sqft", 1, 0).over(w).alias("prev_sqft"),
        F.round((F.col("sqft") / F.sum("sqft").over(w2)), 3).alias("sqft_share"),
    )
    .orderBy(F.col("sqft").desc())
)
