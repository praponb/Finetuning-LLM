from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")

result = (
    df_stores
    .filter(F.col("region") != "North")
    .select(
        "id",
        "region",
        "sqft",
        F.round((F.col("sqft") / F.sum("sqft").over(w)), 3).alias("sqft_share"),
    )
    .orderBy(F.col("sqft").desc(), "region")
)
