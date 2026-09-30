from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")
w2 = Window.partitionBy("city").orderBy("sqft")

result = (
    df_stores
    .filter(F.col("sqft") > 13744)
    .select(
        "id",
        "region",
        "sqft",
        "city",
        F.sum("sqft").over(w).alias("region_sum_sqft"),
        F.rank().over(w2).alias("sqft_rank"),
    )
    .orderBy(F.col("sqft_rank").desc())
)
