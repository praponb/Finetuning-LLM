from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("month", "opened_date"))

result = (
    df_stores
    .filter(F.col("city").isin(["Chiang Mai", "Berlin", "London"]))
    .select(
        "id",
        "opened_date",
        "sqft",
        F.round((F.col("sqft") / F.sum("sqft").over(w)), 3).alias("sqft_share"),
    )
)
