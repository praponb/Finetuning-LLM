from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")

result = (
    df_stores
    .filter(F.col("opened_date") <= "2023-12-01")
    .select(
        "id",
        "city",
        "sqft",
        F.round((F.col("sqft") / F.sum("sqft").over(w)), 3).alias("sqft_share"),
    )
)
