from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region")

result = (
    df_stores
    .select(
        "id",
        "city",
        "region",
        "sqft",
        F.round((F.col("sqft") / F.sum("sqft").over(w)), 3).alias("sqft_share"),
    )
)
